module Keenser.Util where

import           Data.Aeson
import           Data.Attoparsec.ByteString  (Parser, parseOnly)
import qualified Data.ByteString.Char8       as BSC
import qualified Data.ByteString.Lazy        as LBS
import qualified Data.List                   as L
import           Database.Redis              (Redis, lpush)
import           System.Random               (getStdGen, randomRs)

import Keenser.Import
import Keenser.Types

queue :: ToJSON a => Job a -> Redis ()
queue job = void $ lpush ("queue:" <> jobQueue job) [LBS.toStrict $ encode job]

randomHex :: Int -> IO String
randomHex n = map (digits !!) . take n . randomRs (0, 15) <$> getStdGen

digits :: [Char]
digits = ['0' .. '9'] ++ ['A' .. 'F']

mkJob :: MonadIO m => Worker m a -> a -> m (Job a)
mkJob Worker{..} args = liftIO $ do
  _id <- BSC.pack <$> randomHex 12
  now <- getCurrentTime
  return $! Job workerName args _id True now workerQueue

asJSON :: ToJSON a => a -> Object
asJSON a = case toJSON a of
  Object o -> o
  _ -> error "FIXME: define Job -> Object mapping directly"

repeatUntil :: Monad m => m Bool -> m () -> m ()
repeatUntil check act = do
  done <- check
  unless done $ do
    act
    repeatUntil check act

rightToMaybe :: Either a b -> Maybe b
rightToMaybe (Right b) = Just b
rightToMaybe _ = Nothing

trim :: Eq a => [a] -> [a] -> [a]
trim pre corp = fromMaybe corp $ L.stripPrefix pre corp

parseMaybe :: Parser a -> BSC.ByteString -> Maybe a
parseMaybe p = rightToMaybe . parseOnly p

boolToRedis :: Bool -> BSC.ByteString
boolToRedis True  = "true"
boolToRedis False = "false"
