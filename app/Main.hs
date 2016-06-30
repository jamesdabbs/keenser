{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Main where

import Lib

import Control.Concurrent
import Control.Monad
import Control.Monad.Logger
import qualified Data.ByteString as BS
import Data.Monoid
import qualified Data.Text as T
import Database.Redis
import System.Log.FastLogger

s :: Show a => a -> T.Text
s = T.pack . show

instance MonadLogger IO where
  monadLoggerLog loc src lvl msg = void . forkIO . BS.putStr . fromLogStr . defaultLogStr loc src lvl $ toLogStr msg

count :: Worker IO Int
count = Worker "count" "default" $ \n ->
  forM_ [1 .. n] $ \i -> do
    $(logInfo) $ s i <> " / " <> s n
    sleep 1

crash :: Worker IO ()
crash = Worker "crash" "default" $ \_ -> do
  $(logInfo) "one ..."
  sleep 1
  $(logInfo) "two ..."
  sleep 1
  $(logInfo) "five ..."
  sleep 1
  error "Three, sir"

main :: IO ()
main = do
  conn <- connect defaultConnectInfo
  conf <- mkConf conn $ do
    concurrency 2
    register count
    register crash

  m <- startProcess conf

  let loop = do
        enqueue m count 10
        enqueue m count 7
        enqueue m count 19
        enqueue m crash ()
        t <- getLine
        unless (t == "q") loop
  loop
  -- await m
