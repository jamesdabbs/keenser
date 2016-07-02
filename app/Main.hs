{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}
module Main where

import Keenser

import Control.Concurrent
import Control.Monad
import Control.Monad.Logger
import Data.Aeson
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

nope :: Worker IO Int
nope = Worker "nope" "default" $ \n -> do
  $(logError) $ "Noping out " <> s n
  error $ "NOPE " ++ show n

notify :: T.Text -> Middleware IO
notify str m w j q run = do
  $(logDebug) $ str <> " - starting job " <> s q <> " " <> s (encode j)
  run
  $(logDebug) $ str <> " - done"

noop :: Middleware IO
noop m w j q run = run

defaults :: [a] -> [a] -> [a]
defaults (a:as) (b:bs) = b : defaults as bs
defaults as [] = as
defaults _ _ = []

main :: IO ()
main = do
  args <- getArgs
  let par:counts:crashes:_ = defaults [5,20,0] $ map read args

  putStrLn $ show par ++ " workers, " ++ show counts ++ " passes, "  ++ show crashes ++ " fails"

  conn <- connect defaultConnectInfo
  conf <- mkConf conn $ do
    middleware record
    middleware retry
    -- middleware $ notify "[Middleware 1] "
    -- middleware $ notify "[Middleware 2] "
    concurrency par
    register count
    register crash
    register nope

  m <- startProcess conf

  -- enqueueIn 60 m count 3
  enqueue m nope 42

  -- replicateM_ crashes $ enqueue m crash ()
  -- forM_ [1 .. counts] $ enqueue m count

  forkIO . forever $ do
    t <- getLine
    if t == "q"
      then do
        putStrLn "Stopping"
        halt m
      else putStrLn "Press `q` to quit"

  await m
