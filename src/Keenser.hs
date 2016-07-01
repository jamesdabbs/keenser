{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE TemplateHaskell      #-}
module Keenser
  ( Manager
  , Middleware
  , Worker(..)
  , await
  , concurrency
  , halt
  , enqueue
  , enqueueAt
  , enqueueIn
  , mkConf
  , middleware
  , monitor
  , register
  , retry
  , sleep
  , startProcess
  ) where

import           Control.Concurrent          (ThreadId)
import           Control.Concurrent.Lifted   (fork, forkFinally, killThread, myThreadId, threadDelay)
import           Control.Concurrent.STM      (atomically)
import           Control.Concurrent.STM.TVar (TVar, newTVarIO, modifyTVar', swapTVar, readTVarIO, writeTVar)
import           Control.Exception.Lifted    (SomeException, catch)
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
import qualified Data.Set                    as S
import qualified Data.Scientific             as SC
import qualified Data.Text                   as T
import           Data.Text.Encoding          (decodeUtf8, encodeUtf8)
import           Data.Time.Format            (defaultTimeLocale, formatTime, parseTimeOrError)
import           Database.Redis              hiding (decode)
import           Network.HostName            (getHostName)
import           System.Posix.Process        (getProcessID)
import           System.Posix.Types          (CPid(..))

import           Data.Random
import           Data.Random.Source.DevRandom
import           Data.Random.Extras

import Debug.Trace

import Keenser.Middleware
import Keenser.Types
import Keenser.Util

-- TODO: check for and handle Redis errors throughout

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

queue :: ToJSON a => Job a -> Redis ()
queue job = void $ lpush ("queue:" <> jobQueue job) [LBS.toStrict $ encode job]

mkJob :: MonadIO m => Worker m a -> a -> m (Job a)
mkJob Worker{..} args = liftIO $ do
  _id <- BSC.pack <$> randomHex 12
  now <- getCurrentTime
  return $! Job workerName args _id True now workerQueue

enqueue :: (ToJSON a, MonadIO m) => Manager -> Worker m a -> a -> m ()
enqueue Manager{..} w args = do
  job <- mkJob w args
  liftIO . void . runRedis managerRedis $ queue job

enqueueAt :: (ToJSON a, MonadIO m) => UTCTime -> Manager -> Worker m a -> a -> m ()
enqueueAt at Manager{..} w args = do
  job <- mkJob w args
  liftIO . void . runRedis managerRedis $ zadd "schedule" [(timeToDouble at, LBS.toStrict $ encode job)]

enqueueIn :: (ToJSON a, MonadIO m) => Rational -> Manager -> Worker m a -> a -> m ()
enqueueIn snds m w args = do
  now <- liftIO getCurrentTime
  enqueueAt (snds `secondsFrom` now) m w args


randomHex :: Int -> IO String
randomHex n = runRVar (choices n digits) DevRandom
  where digits = ['0' .. '9'] ++ ['A' .. 'F']

-- TODO:
-- - handle the various failure modes here (timeout, can't match worker, can't get work)
-- - if doWork crashes, we should requeue the job, kill this thread, and restart a new processor
startProcessor :: (MonadBaseControl IO m, MonadIO m, MonadLogger m)
               => Config m -> Manager -> m ()
startProcessor Config{..} m = repeatUntil (managerStopping m) $ do
  let mqs = map (\q -> "queue:" <> q) $ managerQueues m
  ejob <- liftIO . runRedis kRedis $ brpop mqs redisTimeout
  case ejob of
    Right (Just (q, jjob)) -> case dispatch kWorkers jjob of
      Nothing -> $(logError) $ "could not find worker for " <> decodeUtf8 jjob
      Just (worker, job) -> runMiddleware (kMiddleware ++ [doWork]) m worker job q
    _ -> $(logDebug) "Nothing to do here"

repeatUntil :: Monad m => m Bool -> m () -> m ()
repeatUntil check act = do
  done <- check
  unless done $ do
    act
    repeatUntil check act

dispatch :: M.Map WorkerName (Worker m Value) -> BS.ByteString -> Maybe (Worker m Value, Job Value)
dispatch workers payload = do
  jv     <- decode $ LBS.fromStrict payload
  worker <- M.lookup (jobClass jv) workers
  return (worker, jv)

rightToMaybe :: Either a b -> Maybe b
rightToMaybe (Right b) = Just b
rightToMaybe _ = Nothing

doWork :: (MonadBaseControl IO m, MonadIO m, ToJSON a)
       => Manager -> Worker m a -> Job a -> Queue -> m () -> m ()
doWork Manager{..} worker job q _ = workerPerform worker $ jobArgs job

redisTimeout :: Integer
redisTimeout = 30

listenForSignals :: (MonadLogger m, MonadBaseControl IO m, MonadIO m) => Manager -> m ()
listenForSignals m@Manager{..} = void . fork . forever $ do
  esig <- liftIO . runRedis managerRedis $ brpop [managerIdentity <> "-signals"] redisTimeout
  case esig of
    Right (Just (_, sig)) -> handleSignal m sig
    _ -> return ()

handleSignal :: (MonadLogger m, MonadBaseControl IO m, MonadIO m) => Manager -> BS.ByteString -> m ()
handleSignal m sig
  | sig == "USR1" = quiet m
  | sig == "TERM" = halt m
  | otherwise     = $(logError) $ "Unrecognized signal: " <> decodeUtf8 sig

startProcess :: (MonadBaseControl IO m, MonadIO m, MonadLogger m) => Config m -> m Manager
startProcess c@Config{..} = do
  m <- mkManager c
  startBeat m
  listenForSignals m
  startPolling m

  -- Start up concurrency many processor threads
  replicateM_ kConcurrency . forkWatch m "processor" $ startProcessor c m
  return m

startPolling :: (MonadBaseControl IO m, MonadLogger m, MonadIO m) => Manager -> m ()
startPolling m@Manager{..} = forkWatch m "poller" . forever $ do
  forM_ ["schedule", "retry"] $ \q -> do
    er <- liftIO $ do
      now <- getCurrentTime
      runRedis managerRedis $ zrangebyscoreLimit q 0 (timeToDouble now) 0 1
    case er of
      Left err -> $(logError) $ "Polling error " <> T.pack (show err)
      Right jobs -> forM_ jobs $ liftIO . runRedis managerRedis . requeueFrom q
  liftIO $ sleep 5 -- TODO: adaptive poll interval

requeueFrom :: Queue -> BS.ByteString -> Redis ()
requeueFrom q payload = case decode $ LBS.fromStrict payload of
  Just job -> do
    zrem q [payload]
    queue (job :: Job Value)
  _ -> return ()


mkManager :: MonadIO m => Config m -> m Manager
mkManager Config{..} = liftIO $ do
  now      <- getCurrentTime
  host     <- getHostName
  CPid pid <- getProcessID
  nonce    <- randomHex 6

  let key = BSC.pack $ concat [host, ".", show pid, ".", nonce]

  Manager (BSC.pack host) now pid kConcurrency kQueues [] key kRedis
    <$> newTVarIO M.empty
    <*> newTVarIO 0
    <*> newTVarIO 0
    <*> newTVarIO False

startBeat :: (MonadBaseControl IO m, MonadLogger m, MonadIO m) => Manager -> m ThreadId
startBeat m@Manager{..} = forkFinally (heartBeat m) $ \e -> do
  case e of
    Left err -> $(logError) $ "Heartbeat error " <> T.pack (show e)
    _        -> $(logDebug) $ "Stopping heartbeat"

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
      incrby "stat:processed" dones
      incrby ("stat:complete:" <> daystamp now) dones
      incrby "stat:failed" fails
      incrby ("stat:failed:" <> daystamp now) dones
      expire managerIdentity 60
    sleep 5

forkWatch :: (MonadLogger m, MonadBaseControl IO m, MonadIO m) => Manager -> T.Text -> m () -> m ()
forkWatch m name a = do
  $(logInfo) $ "starting " <> name
  void $ forkFinally a cleanup
  where
    cleanup _ = $(logDebug) $ name <> " exited"

await :: Manager -> IO ()
await m = do
  repeatUntil (managerStopping m) $ sleep 2
  putStrLn "Shutting down ..."
  sleep 2

sleep :: Int -> IO ()
sleep n = threadDelay $ n * 1000000

halt :: (MonadBaseControl IO m, MonadIO m) => Manager -> m ()
halt m@Manager{..} = do
  quiet m
  running <- liftIO $ readTVarIO managerRunning
  mapM_ killThread $ M.keys running

quiet :: MonadIO m => Manager -> m ()
quiet Manager{..} = liftIO . atomically $ writeTVar managerQuiet True

managerStopping :: MonadIO m => Manager -> m Bool
managerStopping = liftIO . readTVarIO . managerQuiet
