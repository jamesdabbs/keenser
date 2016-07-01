{-# LANGUAGE FlexibleContexts     #-}
module Keenser.Middleware
  ( monitor
  , middleware
  , retry
  , runMiddleware
  ) where

import           Control.Concurrent.Lifted   (fork, forkFinally, killThread, myThreadId, threadDelay)
import           Control.Concurrent.STM      (atomically)
import           Control.Concurrent.STM.TVar (newTVarIO, modifyTVar', swapTVar, readTVarIO, writeTVar)
import           Control.Exception.Lifted    (SomeException, catch, throwIO)
import qualified Data.ByteString.Lazy        as LBS
import qualified Data.Map                    as M
import           Database.Redis

import Keenser.Types
import Keenser.Util

middleware :: Monad m => Middleware m -> Configurator m
middleware m = modify $ \c -> c { kMiddleware = m : kMiddleware c }

runMiddleware :: Monad m => [Middleware m] -> Manager -> Worker m Value -> Job Value -> Queue -> m ()
runMiddleware (t:ts) m w j q = t m w j q $ runMiddleware ts m w j q
runMiddleware _ _ _ _ _ = return ()

retry :: (MonadBaseControl IO m, MonadIO m) => Middleware m
retry Manager{..} _ job _ inner = catch inner $ \e ->
  nextRetry e job >>= \case
    Just (at, rJob) -> void . liftIO $ do
      runRedis managerRedis $ zadd "retry" [(timeToDouble at, LBS.toStrict $ encode rJob)]
    Nothing -> return () -- job is dead

nextRetry :: MonadIO m => SomeException -> Job Value -> m (Maybe (UTCTime, Job Value))
nextRetry ex old = do
  now <- liftIO getCurrentTime
  -- TODO: next job, or dead after some number of tries
  return $! Just (10 `secondsFrom` now, old)

monitor :: (MonadBaseControl IO m, MonadIO m) => Middleware m
monitor Manager{..} _ job q inner = do
  tid <- liftIO myThreadId
  now <- liftIO getCurrentTime
  let
    run = do
      inner
      liftIO . atomically $ do
        modifyTVar' managerRunning $ M.delete tid
        modifyTVar' managerComplete (+1)

    recordError :: (MonadBaseControl IO m, MonadIO m) => SomeException -> m ()
    recordError e = do
      liftIO . atomically $ do
        modifyTVar' managerRunning $ M.delete tid
        modifyTVar' managerFailed (+1)
      throwIO e

    work = RunningJob (toJSON <$> job) q now tid
  liftIO . atomically . modifyTVar' managerRunning $ M.insert tid work
  catch run recordError
