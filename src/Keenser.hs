{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE TemplateHaskell      #-}
module Keenser
  ( Config
  , Configurator
  , Manager
  , ManagerStatus(..)
  , Middleware
  , Worker(..)
  , await
  , checkStatus
  , concurrency
  , halt
  , enqueue
  , enqueueAt
  , enqueueIn
  , mkConf
  , middleware
  , record
  , register
  , retry
  , sleep
  , startProcess
  ) where

import           Control.Concurrent          (ThreadId)
import           Control.Concurrent.Lifted   (fork, forkFinally, killThread, threadDelay)
import           Control.Concurrent.STM      (atomically)
import           Control.Concurrent.STM.TVar (newTVarIO, swapTVar, readTVarIO, writeTVar)
import           Control.Monad.Logger
import           Control.Monad.Trans.State
import           Data.Aeson
import qualified Data.ByteString             as BS
import qualified Data.ByteString.Char8       as BSC
import qualified Data.ByteString.Lazy        as LBS
import qualified Data.Map                    as M
import qualified Data.Text                   as T
import           Data.Text.Encoding          (decodeUtf8)
import           Database.Redis              hiding (decode)
import           Network.HostName            (getHostName)
import           System.Posix.Process        (getProcessID)
import           System.Posix.Signals
import           System.Posix.Types          (CPid(..))

import Debug.Trace

import Keenser.Import
import Keenser.Middleware
import Keenser.Types
import Keenser.Util


redisTimeout :: Integer
redisTimeout = 30

heartBeatFreq :: Int
heartBeatFreq = 5

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
    w = Worker workerName workerQueue $ \obj ->
      case fromJSON obj of
        Data.Aeson.Success job -> workerPerform job
        Data.Aeson.Error   err -> $(logError) $ "job failed to parse: " <> T.pack (show obj) <> " / " <> T.pack err


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


-- TODO:
-- - handle the various failure modes here (timeout, can't match worker, can't get work)
-- - if doWork crashes, we should requeue the job, kill this thread, and restart a new processor
-- - track down the intermittent "<socket: #>: hFlush: illegal operation (handle is closed)" error from here
startProcessor :: (MonadBaseControl IO m, MonadIO m, MonadLogger m)
               => Config m -> Manager -> m ()
startProcessor Config{..} m = repeatUntil (managerStopping m) $ do
  let mqs = map (\q -> "queue:" <> q) $ managerQueues m
  ejob <- liftIO . runRedis kRedis $ brpop mqs redisTimeout
  case ejob of
    Right (Just (q, jjob)) -> case dispatch kWorkers jjob of
      Just (worker, Object obj, job) ->
        runMiddleware
          kMiddleware m worker obj q
          (workerPerform worker $ jobArgs job)
      _ -> $(logError) $ "could not find worker for " <> decodeUtf8 jjob
    _ -> $(logDebug) "Nothing to do here"


dispatch :: M.Map WorkerName (Worker m Value) -> BS.ByteString -> Maybe (Worker m Value, Value, Job Value)
dispatch workers payload = do
  jobj   <- parseMaybe json payload
  job    <- getJob jobj
  worker <- M.lookup (jobClass job) workers
  return $! (worker, jobj, job)

getJob :: Value -> Maybe (Job Value)
getJob v = case fromJSON v of
  Success j -> Just j
  _         -> Nothing

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

  forM_ [1 .. kConcurrency] $ \n ->
    let name = "processor " <> T.pack (show n)
    in forkWatch m name $ startProcessor c m
  return $! m

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
    let q2 = jobQueue (job :: Job Value)
    lpush q2 [payload]
    zrem q [payload]
    return ()
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

clearRedis :: Manager -> Bool -> IO ()
clearRedis Manager{..} raise = do
  liftIO . void . runRedis managerRedis $ do
    srem "processess" [managerIdentity]
    del [managerIdentity]
  when raise $ do
    putStrLn "Waiting 2 seconds for workers to exit"
    sleep 2
    raiseSignal keyboardSignal

startBeat :: (MonadBaseControl IO m, MonadLogger m, MonadIO m) => Manager -> m ThreadId
startBeat m@Manager{..} = do
  liftIO $ installHandler keyboardSignal (CatchOnce $ clearRedis m True) Nothing

  forkFinally (heartBeat m) $ \e -> do
    case e of
      Left _ -> $(logError) $ "Heartbeat error " <> T.pack (show e)
      _      -> $(logDebug) $ "Stopping heartbeat"
    liftIO $ clearRedis m False

workload :: MonadIO m => Manager -> m [RunningJob]
workload Manager{..} = liftIO $ M.elems <$> readTVarIO managerRunning

workToRedis :: RunningJob -> (BS.ByteString, BS.ByteString)
workToRedis j = (BSC.pack . trim "ThreadId " . show $ rjThread j, LBS.toStrict $ encode j)

heartBeat :: MonadIO m => Manager -> m ()
heartBeat m@Manager{..} = liftIO $ do
  runRedis managerRedis $ do
    _ <- del ["queues"]
    sadd "queues" managerQueues

  forever $ do
    now      <- getCurrentTime
    working  <- workload m
    stopping <- readTVarIO managerQuiet
    dones    <- atomically $ swapTVar managerComplete 0
    fails    <- atomically $ swapTVar managerFailed   0

    -- For what it's worth, an abort between here and the end of the block could cause us to under-report stats
    runRedis managerRedis $ do
      sadd "processes" [managerIdentity]
      hmset managerIdentity
        [ ("beat",  timestamp now)
        , ("info",  LBS.toStrict $ encode m)
        , ("busy",  BSC.pack . show $ length working)
        , ("quiet", boolToRedis stopping)
        ]
      _ <- del [managerIdentity <> ":workers"]
      hmset (managerIdentity <> ":workers") $ map workToRedis working
      incrby "stat:processed" dones
      incrby ("stat:complete:" <> daystamp now) dones
      incrby "stat:failed" fails
      incrby ("stat:failed:" <> daystamp now) dones
      expire managerIdentity 60
    sleep heartBeatFreq

checkStatus :: MonadIO m => Manager -> Connection -> m ManagerStatus
checkStatus Manager{..} conn = liftIO . runRedis conn $ do
  procs  <- smembers "processes"
  info   <- hgetall managerIdentity
  done   <- Database.Redis.get "stat:processed"
  failed <- Database.Redis.get "stat:failed"
  return $! ManagerStatus (d procs []) (d info []) (d' done 0) (d' failed 0)

-- TODO: clean this up
d :: Either Reply a -> a -> a
d (Right a) _ = a
d _ a = a

d' :: Read a => Either Reply (Maybe BSC.ByteString) -> a -> a
d' (Right (Just a)) _ = read $ BSC.unpack a
d' _ a = a

forkWatch :: (MonadLogger m, MonadBaseControl IO m, MonadIO m) => Manager -> T.Text -> m () -> m ()
forkWatch m name a = do
  $(logInfo) $ name <> " starting."
  void $ forkFinally a restart
  where
    restart (Left e) = do
      $(logError) $ name <> " exited " <> T.pack (show e) <> ". Restarting."
      forkWatch m name a
    restart (Right _) = $(logDebug) $ name <> " exited clean."

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
