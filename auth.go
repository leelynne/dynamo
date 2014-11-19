package dynamo

import (
	"time"

	"github.com/crowdmob/goamz/aws"
)

type Auth struct {
	aws.Auth
}

func NewAuth() (Auth, error) {
	auth, err := aws.GetAuth("", "", "", time.Now())
	if err != nil {
		return Auth{}, err
	}
	return Auth{auth}, nil
}
