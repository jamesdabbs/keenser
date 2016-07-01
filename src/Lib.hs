{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE LambdaCase           #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE TypeSynonymInstances #-}
module Lib
  ( Manager
  , Middleware
  , Worker(..)
  , await
  , concurrency
  , halt
  , enqueue
  , mkConf
  , middleware
  , register
  , sleep
  , startProcess
  ) where

import           Control.Concurrent          (ThreadId)
import           Control.Concurrent.Lifted   (fork, forkFinally, killThread, myThreadId, threadDelay)
import           Control.Concurrent.STM      (atomically)
import           Control.Concurrent.STM.TVar (TVar, newTVarIO, modifyTVar', swapTVar, readTVarIO)
import           Control.Exception.Lifted    (SomeException, catch)
import           Control.Monad               (forever, foldM_, forM_, liftM, replicateM_, void)
import           Control.Monad.Base          (MonadBase)
import           Control.Monad.Logger
import           Control.Monad.IO.Class      (MonadIO, liftIO)
import           Control.Monad.Trans.Control (MonadBaseControl)
import           Control.Monad.Trans.Reader  (ReaderT, ask, runReaderT)
import           Control.Monad.Trans.State
import           Data.Aeson
import           Data.Attoparsec             (parseOnly)
import qualified Data.ByteString             as BS
import qualified Data.ByteString.Char8       as BSC
import qualified Data.ByteString.Lazy        as LBS
import           Data.Int                    (Int32)
import           Data.List                   as L
import qualified Data.Map                    as M
import           Data.Maybe                  (fromMaybe)
import           Data.Monoid                 ((<>))
import qualified Data.Set                    as S
import qualified Data.Scientific             as SC
import qualified Data.Text                   as T
import           Data.Text.Encoding          (decodeUtf8, encodeUtf8)
import           Data.Time.Clock             (UTCTime, getCurrentTime)
import           Data.Time.Format            (defaultTimeLocale, formatTime, parseTimeOrError)
import           Database.Redis              hiding (decode)
import           Network.HostName            (getHostName)
import           System.Posix.Process        (getProcessID)
import           System.Posix.Types          (CPid(..))

import           Data.Random
import           Data.Random.Source.DevRandom
import           Data.Random.Extras

import Debug.Trace

type JobId      = BS.ByteString
type Queue      = BS.ByteString
type WorkerName = BS.ByteString

data Worker m a = Worker
  { workerName    :: WorkerName
  , workerQueue   :: Queue
  , workerPerform :: a -> m ()
  }

(>$<) :: (a -> b) -> Worker m b -> Worker m a
f >$< (Worker name q perform) = Worker name q (perform . f)

data Config m = Config
  { kWorkers     :: M.Map WorkerName (Worker m Value)
  , kQueues      :: [Queue]
  , kConcurrency :: Int
  , kRedis       :: Connection
  -- TODO: I don't love that this all operates on JSON Values rather than on
  --   well-typed arguments
  , kMiddleware  :: [Middleware m]
  }

data Manager = Manager
  { managerHostname    :: BS.ByteString
  , managerStartedAt   :: UTCTime
  , managerPid         :: Int32
  , managerConcurrency :: Int
  , managerQueues      :: [Queue]
  , managerLabels      :: [T.Text]
  , managerIdentity    :: BS.ByteString

  , managerRedis       :: Connection
  , managerRunning     :: TVar (M.Map ThreadId RunningJob)
  , managerComplete    :: TVar Integer
  , managerFailed      :: TVar Integer
  }

data Job a = Job
  { jobClass      :: WorkerName
  , jobArgs       :: a
  , jobId         :: JobId
  , jobRetry      :: Bool
  , jobEnqueuedAt :: UTCTime
  }

instance Functor Job where
  fmap f (Job klass args _id retry at) = Job klass (f args) _id retry at

data RunningJob = RunningJob
  { rjPayload :: Job Value
  , rjQueue   :: Queue
  , rjRunAt   :: UTCTime
  , rjThread  :: ThreadId
  }

type Middleware m = (Worker m Value -> Job Value -> Queue -> m ())
                  -> Worker m Value -> Job Value -> Queue -> m ()

instance ToJSON Manager where
  toJSON Manager{..} = object
    [ "hostname"    .= managerHostname
    , "started_at"  .= (number $ timestamp managerStartedAt)
    , "pid"         .= managerPid
    -- TODO: tag ~ "app"
    , "concurrency" .= managerConcurrency
    , "queues"      .= managerQueues
    -- TODO: labels ~ []
    , "identity"    .= managerIdentity
    ]

instance ToJSON WorkerName where
  toJSON = Data.Aeson.String . decodeUtf8

instance FromJSON WorkerName where
  parseJSON = withText "worker_name" $ return . encodeUtf8

instance ToJSON a => ToJSON (Job a) where
  toJSON Job{..} = object
    [ "class"       .= jobClass
    , "args"        .= jobArgs
    , "jid"         .= jobId
    , "retry"       .= jobRetry
    , "enqueued_at" .= (number $ timestamp jobEnqueuedAt)
    ]

instance ToJSON RunningJob where
  toJSON RunningJob{..} = object
    [ "queue"   .= rjQueue
    , "payload" .= fmap (: []) rjPayload
    , "run_at"  .= (number $ timestamp rjRunAt)
    ]

number :: BS.ByteString -> SC.Scientific
number = read . BSC.unpack

showTime :: SC.Scientific -> UTCTime
showTime n = parseTimeOrError True defaultTimeLocale "%s%Q" $ SC.formatScientific SC.Fixed (Just 6) n

instance FromJSON a => FromJSON (Job a) where
  parseJSON = withObject "job" $ \v -> do
    jobClass      <- v .: "class"
    jobArgs       <- v .: "args"
    jobId         <- v .: "jid"
    jobRetry      <- v .: "retry"
    jobEnqueuedAt <- showTime <$> v .: "enqueued_at"
    return Job{..}

type Configurator m = StateT (Config m) m ()

mkConf :: Monad m => Connection -> Configurator m -> m (Config m)
mkConf conn conf = execStateT conf Config
  { kWorkers     = M.empty
  , kQueues      = ["default"]
  , kConcurrency = 25
  , kRedis       = conn
  , kMiddleware  = []
  }

concurrency :: Monad m => Int -> Configurator m
concurrency n = modify $ \c -> c { kConcurrency = n }

middleware :: Monad m => [Middleware m] -> Configurator m
middleware m = modify $ \c -> c { kMiddleware = m }


unJSON j = case fromJSON j of
  Data.Aeson.Success a -> a
  Data.Aeson.Error str -> error $ "could not unJSON " ++ str

-- TODO:
-- - verify that worker names are unique
-- - update queue list (or at least warn if there are queues that aren't being covered)
register :: (MonadLogger m, MonadIO m, FromJSON a) => Worker m a -> Configurator m
register Worker{..} = modify $ \c -> c { kWorkers = M.insert workerName w $ kWorkers c }
  where
    w = Worker workerName workerQueue $ \value -> do
      case fromJSON value of
        Data.Aeson.Success job -> workerPerform job
        Data.Aeson.Error   err -> $(logError) $ "job failed to parse: " <> T.pack (show value) <> " / " <> T.pack err

enqueue :: ToJSON a => Manager -> Worker m a -> a -> IO ()
enqueue Manager{..} Worker{..} args = do
  _id <- randomHex 12
  now <- getCurrentTime
  let job = Job workerName args _id True now
  er <- runRedis managerRedis $ lpush ("queue:" <> workerQueue) [LBS.toStrict $ encode job]
  case er of
    Left err -> error "enqueue error"
    Right _  -> return ()

randomHex :: Int -> IO BS.ByteString
randomHex n = BSC.pack <$> runRVar (choices n digits) DevRandom
  where digits = ['0' .. '9'] ++ ['A' .. 'F']

-- TODO:
-- - handle the various failure modes here (timeout, can't match worker, can't get work)
-- - if doWork crashes, we should requeue the job, kill this thread, and restart a new processor
startProcessor :: (MonadBaseControl IO m, MonadIO m, MonadLogger m)
               => Config m -> Manager -> m ()
startProcessor Config{..} m = forever $ do
  let mqs = map (\q -> "queue:" <> q) $ managerQueues m
  ejob <- liftIO . runRedis kRedis $ brpop mqs 30
  case ejob of
    Right (Just (q, jjob)) -> case dispatch kWorkers jjob of
      Nothing -> $(logError) $ "could not find worker for " <> decodeUtf8 jjob
      Just (worker, job) -> applyMiddleware kMiddleware (doWork m) worker job q
    _ -> $(logDebug) "Nothing to do here"

applyMiddleware :: [Middleware m] -> Middleware m
applyMiddleware = L.foldr (.) id

dispatch :: M.Map WorkerName (Worker m Value) -> BS.ByteString -> Maybe (Worker m Value, Job Value)
dispatch workers payload = do
  jv     <- decode $ LBS.fromStrict payload
  worker <- M.lookup (jobClass jv) workers
  return (worker, jv)

rightToMaybe :: Either a b -> Maybe b
rightToMaybe (Right b) = Just b
rightToMaybe _ = Nothing

doWork :: (MonadBaseControl IO m, MonadIO m, ToJSON a)
       => Manager -> Worker m a -> Job a -> Queue -> m ()
doWork Manager{..} worker job q = do
  tid <- liftIO myThreadId
  now <- liftIO getCurrentTime
  let
    -- m = show tid ++ ": " ++ show (encode job)
    run = do
      -- traceM $ "starting " ++ m
      workerPerform worker $ jobArgs job
      liftIO . atomically $ do
        -- traceM $ "done & clearing " ++ m
        modifyTVar' managerRunning $ M.delete tid
        modifyTVar' managerComplete (+1)

    recordError :: MonadIO m => SomeException -> m ()
    recordError e = do
      liftIO . atomically $ do
        -- traceM $ "failed & clearing " ++ m
        modifyTVar' managerRunning $ M.delete tid
        modifyTVar' managerFailed (+1)

    work = RunningJob (toJSON <$> job) q now tid
  liftIO . atomically . modifyTVar' managerRunning $ M.insert tid work
  catch run recordError


startProcess :: (MonadBaseControl IO m, MonadIO m, MonadLogger m) => Config m -> m Manager
startProcess c@Config{..} = do
  m <- mkManager c
  startBeat m

  -- Start up concurrency many processor threads
  replicateM_ kConcurrency . forkMonitored m $ startProcessor c m
  return m

mkManager :: MonadIO m => Config m -> m Manager
mkManager Config{..} = liftIO $ do
  now      <- getCurrentTime
  host     <- getHostName
  CPid pid <- getProcessID
  nonce    <- randomHex 6
  busy     <- newTVarIO M.empty
  complete <- newTVarIO 0
  failed   <- newTVarIO 0

  let key = BSC.pack $ concat [host, ".", show pid, ".", BSC.unpack nonce]

  return Manager
    { managerHostname    = BSC.pack host
    , managerStartedAt   = now
    , managerPid         = pid
    , managerConcurrency = kConcurrency
    , managerQueues      = kQueues
    , managerLabels      = [] -- TODO
    , managerIdentity    = key
    , managerRedis       = kRedis
    , managerRunning     = busy
    , managerComplete    = complete
    , managerFailed      = failed
    }

startBeat :: (MonadBaseControl IO m, MonadLogger m, MonadIO m) => Manager -> m ThreadId
startBeat m@Manager{..} = forkFinally (heartBeat m) $ \e -> do
  case e of
    Left err -> $(logError) $ "Heartbeat error " <> T.pack (show e)
    _ -> return ()
  liftIO . void . runRedis managerRedis $ do
    srem "processess" [managerIdentity]
    del [managerIdentity]

workload :: MonadIO m => Manager -> m [RunningJob]
workload Manager{..} = liftIO $ M.elems <$> readTVarIO managerRunning

workToRedis :: RunningJob -> (BS.ByteString, BS.ByteString)
workToRedis j = (BSC.pack . trim "ThreadId " . show $ rjThread j, LBS.toStrict $ encode j)

trim :: Eq a => [a] -> [a] -> [a]
trim pre corp = fromMaybe corp $ stripPrefix pre corp

heartBeat :: MonadIO m => Manager -> m ()
heartBeat m@Manager{..} = liftIO $ do
  runRedis managerRedis $ do
    _ <- del ["queues"]
    sadd "queues" managerQueues

  forever $ do
    now     <- getCurrentTime
    working <- workload m
    dones   <- atomically $ swapTVar managerComplete 0
    fails   <- atomically $ swapTVar managerFailed   0

    -- For what it's worth, an abort between here and the end of the block could cause us to under-report stats
    runRedis managerRedis $ do
      sadd "processes" [managerIdentity]
      hmset managerIdentity
        [ ("beat",  timestamp now)
        , ("info",  LBS.toStrict $ encode m)
        , ("busy",  BSC.pack . show $ length working)
        , ("quiet", "false") -- TODO should be true iff the worker has stopped taking jobs
        ]
      _ <- del [managerIdentity <> ":workers"]
      hmset (managerIdentity <> ":workers") $ map workToRedis working
      -- traceM $ "dones " ++ show dones
      incrby "stat:complete" dones
      incrby ("stat:complete:" <> day now) dones
      incrby "stat:failed" fails
      incrby ("stat:failed:" <> day now) dones
      expire managerIdentity 60
    sleep 5

timestamp :: UTCTime -> BS.ByteString
timestamp = BSC.pack . formatTime defaultTimeLocale "%s%Q"

day :: UTCTime -> BS.ByteString
day = BSC.pack . formatTime defaultTimeLocale "%Y-%m-%d"

forkMonitored :: (MonadBaseControl IO m, MonadIO m) => Manager -> m () -> m ()
forkMonitored m a = do
  void $ forkFinally run cleanup
  where
    run       = a
    cleanup _ = liftIO $ putStrLn "fork exited"

await :: Manager -> IO ()
await _ = do
  putStrLn "Press enter to exit"
  getLine
  return ()

sleep :: Int -> IO ()
sleep n = threadDelay $ n * 1000000

halt :: (MonadBase IO m, MonadIO m) => Manager -> m ()
halt Manager{..} = do
  running <- liftIO $ readTVarIO managerRunning
  mapM_ killThread $ M.keys running
