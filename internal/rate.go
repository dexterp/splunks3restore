package internal

import (
	"context"
	"golang.org/x/time/rate"
	"log"
)

const AWSDefaultRate = float64(256)

var AWSRate *rate.Limiter

func AWSRateLimit() {
	if AWSRate != nil {
		err := AWSRate.Wait(context.TODO())
		if err != nil {
			log.Println(err.Error())
		}
	}
}

func SetupAWSRateLimit(defaultrate float64) {
	if AWSRate == nil {
		var l float64
		switch {
		case Config.RateLimit < 0:
			AWSRate = nil
		case Config.RateLimit == 0:
			l = defaultrate
		default:
			l = Config.RateLimit
		}
		AWSRate = rate.NewLimiter(rate.Limit(l), 512)
	}
}
