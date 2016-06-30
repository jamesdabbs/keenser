{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE TypeSynonymInstances #-}
module Lib
  ( Manager
  , Worker(..)
  , await
  , concurrency
  , enqueue
  , mkConf
  , register
  , sleep
  , startProcess
  ) where

import           Control.Concurrent          (ThreadId)
import           Control.Concurrent.STM.TVar
import           Control.Concurrent.Lifted   (fork, forkFinally, myThreadId, threadDelay)
import           Control.Concurrent.STM      (atomically)
import           Control.Concurrent.STM.TVar (TVar, newTVarIO)
import           Control.Monad               (forever, liftM, replicateM_, void)
import           Control.Monad.Logger
import           Control.Monad.IO.Class      (MonadIO, liftIO)
import           Control.Monad.Trans.Control (MonadBaseControl)
import           Control.Monad.Trans.Reader  (ReaderT, ask, runReaderT)
import           Control.Monad.Trans.State
import           Data.Aeson
import qualified Data.ByteString             as BS
import qualified Data.ByteString.Char8       as BSC
import qualified Data.ByteString.Lazy        as LBS
import           Data.Int                    (Int32)
import           Data.List                   as L
import qualified Data.Map                    as M
import           Data.Monoid                 ((<>))
import qualified Data.Set                    as S
import qualified Data.Text                   as T
import           Data.Text.Encoding          (decodeUtf8, encodeUtf8)
import           Data.Time.Clock             (UTCTime, getCurrentTime)
import           Data.Time.Format            (defaultTimeLocale, formatTime)
import           Database.Redis              hiding (decode)
import           Network.HostName            (getHostName)
import           System.Posix.Process        (getProcessID)
import           System.Posix.Types          (CPid(..))

import           Data.Random
import           Data.Random.Source.DevRandom
import           Data.Random.Extras

type JobId      = BS.ByteString
type Queue      = BS.ByteString
type WorkerName = BS.ByteString

data Worker m a = Worker
  { workerName    :: WorkerName
  , workerQueue   :: Queue
  , workerPerform :: a -> m ()
  }

data Config m = Config
  { kWorkers     :: M.Map WorkerName (Worker m BS.ByteString)
  , kQueues      :: [Queue]
  , kConcurrency :: Int
  , kRedis       :: Connection
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
  , managerRunning     :: TVar (S.Set ThreadId)
  }

data Job a = Job
  { jobClass      :: WorkerName
  , jobArgs       :: a
  , jobId         :: JobId
  , jobRetry      :: Bool
  , jobEnqueuedAt :: UTCTime
  }

instance ToJSON Manager where
  toJSON Manager{..} = object
    [ "hostname"    .= managerHostname
    , "started_at"  .= timestamp managerStartedAt
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
    , "enqueued_at" .= jobEnqueuedAt
    ]

instance FromJSON a => FromJSON (Job a) where
  parseJSON = withObject "job" $ \v -> do
    jobClass      <- v .: "class"
    jobArgs       <- v .: "args"
    jobId         <- v .: "jid"
    jobRetry      <- v .: "retry"
    jobEnqueuedAt <- v .: "enqueued_at"
    return Job{..}

type Configurator m = StateT (Config m) m ()

mkConf :: Monad m => Connection -> Configurator m -> m (Config m)
mkConf conn conf = execStateT conf Config
  { kWorkers     = M.empty
  , kQueues      = ["queue:default"]
  , kConcurrency = 25
  , kRedis       = conn
  }

concurrency :: Monad m => Int -> Configurator m
concurrency n = modify $ \c -> c { kConcurrency = n }

-- TODO:
-- - verify that worker names are unique
-- - update queue list (or at least warn if there are queues that aren't being covered)
register :: (Monad m, FromJSON a) => Worker m a -> Configurator m
register Worker{..} = modify $ \c -> c { kWorkers = M.insert workerName w $ kWorkers c }
  where
    w = Worker workerName workerQueue $ \s ->
      case eitherDecode $ LBS.fromStrict s of
        Right job -> workerPerform $ jobArgs job
        Left  err  -> error $ "job failed to parse: " ++ show s ++ " / " ++ err

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
randomHex n = do
  ds <- runRVar (choices n digits) DevRandom
  return $ BSC.pack ds
  where
    digits = ['0' .. '9'] ++ ['A' .. 'F']

startProcessor :: (MonadBaseControl IO m, MonadIO m, MonadLogger m)
               => Connection
               -> [Queue]
               -> (BS.ByteString -> Maybe (Worker m BS.ByteString))
               -> m ()
startProcessor conn qs dispatcher = forever $ do
  -- TODO: update busy and stats on start / done
  ejob <- liftIO . runRedis conn $ brpop qs 30 -- FIXME: timeout
  case ejob of
    Right (Just (_, jjob)) -> case dispatcher jjob of
      Nothing     -> $(logError) $ "could not find worker for " <> decodeUtf8 jjob
      Just worker -> workerPerform worker jjob
    _ -> $(logError) "could not get work" -- FIXME

startProcess :: (MonadBaseControl IO m, MonadIO m, MonadLogger m) => Config m -> m Manager
startProcess Config{..} = do
  now      <- liftIO getCurrentTime
  host     <- liftIO getHostName
  CPid pid <- liftIO getProcessID
  nonce    <- liftIO $ randomHex 6
  busy     <- liftIO $ newTVarIO S.empty

  let key = BSC.pack $ concat [host, ".", show pid, ".", BSC.unpack nonce]

  let m = Manager
        { managerHostname    = BSC.pack host
        , managerStartedAt   = now
        , managerPid         = pid
        , managerConcurrency = kConcurrency
        , managerQueues      = kQueues
        , managerLabels      = [] -- TODO
        , managerIdentity    = key

        , managerRedis       = kRedis
        , managerRunning     = busy
        }

  -- Heartbeat check
  fork . liftIO . forever $ do
    now   <- getCurrentTime
    count <- readTVarIO busy
    runRedis kRedis $ do
      sadd "processes" [key]
      hmset key
        [ ("beat",  timestamp now)
        , ("info",  LBS.toStrict $ encode m)
        , ("busy",  BSC.pack . show $ count)
        , ("quiet", "false") -- TODO should be true iff the worker has stopped taking jobs
        ]
      expire key 60
    threadDelay $ 5 * 1000000

  -- Start up concurrency many processor threads
  replicateM_ kConcurrency . forkMonitored m . startProcessor kRedis kQueues $ findWorker kWorkers

  return m

timestamp :: UTCTime -> BS.ByteString
timestamp = BSC.pack . formatTime defaultTimeLocale "%s%Q"

findWorker :: M.Map WorkerName (Worker m a) -> BS.ByteString -> Maybe (Worker m a)
findWorker workers payload = case workerClass payload of
  Just klass -> M.lookup klass workers
  Nothing -> Nothing

data Job' = Job' -- TODO: be beter
  { jobKlass :: WorkerName }

instance FromJSON Job' where
  parseJSON = withObject "job'" $ \v -> do
    jobKlass      <- v .: "class"
    return Job'{..}

workerClass :: BS.ByteString -> Maybe WorkerName
workerClass s = jobKlass <$> parsed
  where
    parsed :: Maybe Job'
    parsed = decode $ LBS.fromStrict s

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
