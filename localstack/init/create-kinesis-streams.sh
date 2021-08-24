awslocal kinesis create-stream --stream-name demo --shard-count 1

sleep 5

awslocal kinesis list-streams
awslocal kinesis list-shards --stream-name demo