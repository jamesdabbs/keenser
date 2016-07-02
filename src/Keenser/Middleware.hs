{-# LANGUAGE FlexibleContexts     #-}
module Keenser.Middleware
  ( middleware
  , record
  , retry
  , runMiddleware
  ) where

import           Control.Concurrent.Lifted   (fork, forkFinally, killThread, myThreadId, threadDelay)
import           Control.Concurrent.STM      (atomically)
import           Control.Concurrent.STM.TVar (newTVarIO, modifyTVar', swapTVar, readTVarIO, writeTVar)
import           Control.Exception.Lifted    (SomeException, catch, throwIO)
import           Data.Aeson
import qualified Data.ByteString.Lazy        as LBS
import qualified Data.HashMap.Strict         as HM
import qualified Data.Map                    as M
import qualified Data.Text                   as T
import           Database.Redis

import Keenser.Types
import Keenser.Util

middleware :: Monad m => Middleware m -> Configurator m
middleware m = modify $ \c -> c { kMiddleware = m : kMiddleware c }

runMiddleware :: Monad m
              => [Middleware m]
              -> Manager -> Worker m Value -> Object -> Queue
              -> m ()
              -> m ()
runMiddleware (t:ts) m w j q start = t m w j q $ runMiddleware ts m w j q start
runMiddleware _ _ _ _ _ start = start

retry :: (MonadBaseControl IO m, MonadIO m) => Middleware m
retry Manager{..} _ job q inner = catch inner $ \e -> do
  (count, time, rJob) <- nextRetry e job q
  if count < 25
    then
      void . liftIO. runRedis managerRedis $
        zadd "retry" [(timeToDouble time, LBS.toStrict $ encode rJob)]
    else return () -- send job to dead queue

-- TODO: + rand(30) * (count + 1) to prevent thundering herd
retryTime :: Integer -> UTCTime -> UTCTime
retryTime count start = fromInteger offset `secondsFrom` start
  where offset = (count ^ 4) + 15

todo :: T.Text
todo = "TODO"

mJSON :: FromJSON a => Value -> Maybe a
mJSON v = case fromJSON v of
  Success a -> Just a
  _         -> Nothing

nextRetry :: MonadIO m => SomeException -> Object -> Queue -> m (Integer, UTCTime, Object)
nextRetry ex old q = do
  now <- liftIO getCurrentTime

  let
    -- TODO: I don't love how stringly-typed this direct `Object` manipulation is,
    --   but if we're staying consistent w/ Sidekiq's Redis API, we need to allow
    --   middleware authors to jam whatever metadata they want on the Jobject
    (count, status) = case HM.lookup "retry_count" old >>= mJSON of
      Just n  -> (n+1, ["retried_at" .= timestamp now])
      Nothing -> (  0, ["failed_at"  .= timestamp now])
    updates = HM.fromList $
      [ "queue"         .= fromMaybe q (HM.lookup "retry_queue" old >>= mJSON)
      , "error_message" .= todo
      , "error_class"   .= todo
      , "retry_count"   .= count
      ] ++ status
  return $! (count, retryTime count now, HM.union updates old)

record :: (MonadBaseControl IO m, MonadIO m) => Middleware m
record Manager{..} _ job q inner = do
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

    work = RunningJob job q now tid
  liftIO . atomically . modifyTVar' managerRunning $ M.insert tid work
  catch run recordError
