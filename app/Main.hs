{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Main where

import Lib

import Control.Concurrent
import Control.Monad
import Control.Monad.Logger
import qualified Data.ByteString as BS
import Data.Maybe
import Data.Monoid
import qualified Data.Text as T
import Database.Redis
import System.Environment
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

defaults :: [a] -> [a] -> [a]
defaults (a:as) (b:bs) = b : defaults as bs
defaults as [] = as
defaults _ _ = []

main :: IO ()
main = do
  args <- getArgs
  let par:jobs:_ = defaults [5,20] $ map read args

  putStrLn $ show par ++ " workers, " ++ show jobs ++ " jobs"

  conn <- connect defaultConnectInfo
  conf <- mkConf conn $ do
    concurrency par
    register count
    register crash

  m <- startProcess conf

  loop m jobs
  -- await m

loop m limit = do
  putStrLn $ "Q'ing " ++ show limit ++ " jobs"
  forM_ [1 .. limit] $ enqueue m count
  t <- getLine
  if t == "q"
    then do
      putStrLn "Stopping"
      halt m
    else loop m limit
