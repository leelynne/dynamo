package dynamo

import (
	"time"

	"github.com/crowdmob/goamz/aws"
)

type Auth struct {
	aws.Auth
}

func NewAuth() Auth {
	return Auth{
		aws.GetAuth("", "", "", time.Now()),
	}
}
