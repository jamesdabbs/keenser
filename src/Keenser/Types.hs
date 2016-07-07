{-# LANGUAGE TypeSynonymInstances #-}
module Keenser.Types
  ( Config(..)
  , Configurator
  , Job(..)
  , JobId
  , Manager(..)
  , ManagerStatus(..)
  , Middleware
  , Queue
  , RunningJob(..)
  , Signal(..)
  , Worker(..)
  , WorkerName
  ) where

import           Control.Concurrent          (ThreadId)
import           Control.Concurrent.STM.TVar (TVar)
import           Control.Monad.Trans.State   (StateT)
import           Data.Aeson
import qualified Data.ByteString             as BS
import           Data.Int                    (Int32)
import qualified Data.Map                    as M
import qualified Data.Text                   as T
import           Data.Text.Encoding          (decodeUtf8, encodeUtf8)
import           Database.Redis              (Connection)

import Keenser.Import

data Config m = Config
  { kWorkers     :: M.Map WorkerName (Worker m Value)
  , kQueues      :: [Queue]
  , kConcurrency :: Int
  , kRedis       :: Connection
  , kMiddleware  :: [Middleware m]
  }

type Configurator m = StateT (Config m) m ()

type JobId = BS.ByteString
data Job a = Job
  { jobClass      :: WorkerName
  , jobArgs       :: a
  , jobId         :: JobId
  , jobRetry      :: Bool
  , jobEnqueuedAt :: UTCTime
  , jobQueue      :: Queue
  }

data Manager = Manager
  { managerHostname    :: BS.ByteString
  , managerStartedAt   :: UTCTime
  , managerPid         :: Int32
  , managerConcurrency :: Int
  , managerQueues      :: [Queue]
  , managerLabels      :: [T.Text]
  , managerIdentity    :: BS.ByteString
  , managerRedis       :: Connection

  , managerRunning     :: TVar (M.Map ThreadId RunningJob)
  , managerComplete    :: TVar Integer
  , managerFailed      :: TVar Integer
  , managerQuiet       :: TVar Bool
  }

data ManagerStatus = ManagerStatus
  { sProcs  :: [BS.ByteString]
  , sState  :: [(BS.ByteString, BS.ByteString)]
  , sDone   :: Int
  , sFailed :: Int
  , sQueues :: [(BS.ByteString, Integer)]
  } deriving Show

type Middleware m = Manager -> Worker m Value -> Object -> Queue -> m () -> m ()

type Queue = BS.ByteString

data RunningJob = RunningJob
  { rjPayload :: Object
  , rjQueue   :: Queue
  , rjRunAt   :: UTCTime
  , rjThread  :: ThreadId
  }

data Signal = USR1 | TERM deriving (Show, Read)

type WorkerName = BS.ByteString
data Worker m a = Worker
  { workerName    :: WorkerName
  , workerQueue   :: Queue
  , workerPerform :: a -> m ()
  }


instance Functor Job where
  fmap f (Job klass args _id retry at q) = Job klass (f args) _id retry at q

instance ToJSON a => ToJSON (Job a) where
  toJSON Job{..} = object
    [ "class"       .= jobClass
    , "args"        .= [jobArgs]
    , "jid"         .= jobId
    , "retry"       .= jobRetry
    , "enqueued_at" .= timeToJson jobEnqueuedAt
    , "queue"       .= jobQueue
    ]

instance FromJSON a => FromJSON (Job a) where
  parseJSON = withObject "job" $ \v -> do
    jobClass      <- v .: "class"
    jobArgs       <- head <$> v .: "args"
    jobId         <- v .: "jid"
    jobRetry      <- v .: "retry"
    jobEnqueuedAt <- jsonToTime <$> v .: "enqueued_at"
    jobQueue      <- v .: "queue"
    return $! Job{..}


instance ToJSON Manager where
  toJSON Manager{..} = object
    [ "hostname"    .= managerHostname
    , "started_at"  .= timeToJson managerStartedAt
    , "pid"         .= managerPid
    -- TODO: tag ~ "app"
    , "concurrency" .= managerConcurrency
    , "queues"      .= managerQueues
    -- TODO: labels ~ []
    , "identity"    .= managerIdentity
    ]

instance ToJSON RunningJob where
  toJSON RunningJob{..} = object
    [ "queue"   .= rjQueue
    , "payload" .= fmap (: []) rjPayload -- since Sidekiq stores *args
    , "run_at"  .= timeToJson rjRunAt
    ]

instance ToJSON WorkerName where
  toJSON = Data.Aeson.String . decodeUtf8

instance FromJSON WorkerName where
  parseJSON = withText "worker_name" $ return . encodeUtf8
