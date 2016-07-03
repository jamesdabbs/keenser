{-# LANGUAGE FlexibleContexts #-}
module Keenser.Middleware.Stats
  ( record
  ) where

import           Control.Concurrent.Lifted   (myThreadId)
import           Control.Concurrent.STM      (atomically)
import           Control.Concurrent.STM.TVar (modifyTVar')
import           Control.Exception.Lifted    (SomeException, catch, throwIO)
import qualified Data.Map                    as M

import Keenser.Import
import Keenser.Types

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
