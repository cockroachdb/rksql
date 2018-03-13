package job

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"rubrik/util/log"
)

func artifactID(instance InstanceSpecifier) string {
	jobID, instanceID := instance.JobAndInstance()
	text := fmt.Sprintf("%v:::%v", jobID, instanceID)
	hasher := md5.New()
	hasher.Write([]byte(text))
	hash := hex.EncodeToString(hasher.Sum(nil))
	return fmt.Sprintf("%v###%v", text, hash)
}

type artifactData struct {
	nodeID     string
	jobID      string
	instanceID int64
	hash       string
}

func parsedArtifactID(ctx context.Context, artifactID string) artifactData {
	hashParts := strings.Split(artifactID, "###")
	if len(hashParts) != 2 {
		log.Fatalf(ctx, "Bad artifactID: %v", artifactID)
	}
	hasher := md5.New()
	hasher.Write([]byte(hashParts[0]))
	hash := hex.EncodeToString(hasher.Sum(nil))
	if hash != hashParts[1] {
		log.Fatalf(
			ctx,
			"Mismatch in artifactID hash. expected: %v actual: %v",
			hash,
			hashParts,
		)
	}
	instanceParts := strings.Split(hashParts[0], ":::")
	if len(instanceParts) != 2 {
		panic(fmt.Sprintf("bad artifactID: %v", artifactID))
	}
	instanceID, err := strconv.ParseInt(instanceParts[1], 10, 64)
	if err != nil {
		panic(fmt.Sprintf("bad instance id: %v err: %v", instanceParts[2], err))
	}
	return artifactData{
		jobID:      instanceParts[0],
		instanceID: instanceID,
		hash:       hashParts[1],
	}
}
