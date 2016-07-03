{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell  #-}
module Keenser.Middleware
  ( middleware
  , record
  , retry
  , runMiddleware
  ) where

import Data.Aeson

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
