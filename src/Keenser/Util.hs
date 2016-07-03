module Keenser.Util where

import           Data.Aeson
import           Data.AffineSpace            ((.+^))
import           Data.Attoparsec.ByteString  (Parser, parseOnly)
import qualified Data.ByteString             as BS
import qualified Data.ByteString.Char8       as BSC
import qualified Data.ByteString.Lazy        as LBS
import qualified Data.List                   as L
import           Data.Thyme.Clock            (fromSeconds)
import           Data.Thyme.Format           (formatTime, readTime)
import qualified Data.Scientific             as SC
import           Database.Redis              (Redis, lpush)
import           System.Locale               (defaultTimeLocale)

import           Data.Random
import           Data.Random.Source.DevRandom
import           Data.Random.Extras

import Keenser.Import
import Keenser.Types

queue :: ToJSON a => Job a -> Redis ()
queue job = void $ lpush ("queue:" <> jobQueue job) [LBS.toStrict $ encode job]

randomHex :: Int -> IO String
randomHex n = runRVar (choices n digits) DevRandom
  where digits = ['0' .. '9'] ++ ['A' .. 'F']

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
