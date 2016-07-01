module Keenser.Util
  ( module Keenser.Util
  ) where

import Control.Monad               as Keenser.Util
import Control.Monad.IO.Class      as Keenser.Util (MonadIO, liftIO)
import Control.Monad.Trans.Control as Keenser.Util (MonadBaseControl)
import Control.Monad.Trans.State   as Keenser.Util (modify)
import Data.Aeson                  as Keenser.Util (Value, toJSON, fromJSON, parseJSON, encode, decode)
import Data.Maybe                  as Keenser.Util (fromMaybe)
import Data.Monoid                 as Keenser.Util ((<>))
import Data.Thyme.Clock            as Keenser.Util (UTCTime, getCurrentTime)

import           Data.AffineSpace            ((.+^))
import qualified Data.ByteString             as BS
import qualified Data.ByteString.Char8       as BSC
import           Data.Thyme.Clock            (fromSeconds)
import           Data.Thyme.Format           (formatTime, readTime)
import qualified Data.Scientific             as SC
import           System.Locale               (defaultTimeLocale)


timestamp :: UTCTime -> BS.ByteString
timestamp = BSC.pack . formatTime defaultTimeLocale "%s%Q"

daystamp :: UTCTime -> BS.ByteString
daystamp = BSC.pack . formatTime defaultTimeLocale "%Y-%m-%d"

timeToJson :: UTCTime -> SC.Scientific
timeToJson = read . BSC.unpack . timestamp

jsonToTime :: SC.Scientific -> UTCTime
jsonToTime = readTime defaultTimeLocale "%s%Q" . SC.formatScientific SC.Fixed (Just 6)

timeToDouble :: UTCTime -> Double
timeToDouble = read . formatTime defaultTimeLocale "%s%Q"

secondsFrom :: Rational -> UTCTime -> UTCTime
secondsFrom n time = time .+^ fromSeconds n
