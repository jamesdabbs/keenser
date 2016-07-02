{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell  #-}
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
import           Control.Monad.Logger
import           Data.Aeson
import qualified Data.ByteString.Lazy        as LBS
import qualified Data.HashMap.Strict         as HM
import qualified Data.Map                    as M
import qualified Data.Text                   as T
import           Database.Redis

import Keenser.Types
import Keenser.Import

import Keenser.Middleware.Retry
import Keenser.Middleware.Stats

middleware :: Monad m => Middleware m -> Configurator m
middleware m = modify $ \c -> c { kMiddleware = m : kMiddleware c }

runMiddleware :: Monad m
              => [Middleware m]
              -> Manager -> Worker m Value -> Object -> Queue
              -> m ()
              -> m ()
runMiddleware (t:ts) m w j q start = t m w j q $ runMiddleware ts m w j q start
runMiddleware _ _ _ _ _ start = start
