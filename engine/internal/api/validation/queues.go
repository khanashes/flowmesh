package validation

import (
	"fmt"
	"time"
)

// ValidateEnqueueRequest validates an enqueue request
func ValidateEnqueueRequest(resourcePath string, payload []byte, delaySeconds int64) error {
	// Resource path validation is done at API layer via BuildResourcePath

	if len(payload) == 0 {
		return fmt.Errorf("payload cannot be empty")
	}

	if delaySeconds < 0 {
		return fmt.Errorf("delay_seconds cannot be negative")
	}

	// Max delay: 1 year
	maxDelay := int64(365 * 24 * 60 * 60)
	if delaySeconds > maxDelay {
		return fmt.Errorf("delay_seconds cannot exceed %d (1 year)", maxDelay)
	}

	return nil
}

// ValidateReserveRequest validates a reserve request
func ValidateReserveRequest(resourcePath string, visibilityTimeoutSeconds int64, longPollTimeoutSeconds int64) error {
	// Resource path validation is done at API layer via BuildResourcePath

	// Visibility timeout: 1 second to 12 hours
	if visibilityTimeoutSeconds > 0 {
		if visibilityTimeoutSeconds < 1 {
			return fmt.Errorf("visibility_timeout_seconds must be at least 1")
		}
		maxVisibilityTimeout := int64(12 * 60 * 60) // 12 hours
		if visibilityTimeoutSeconds > maxVisibilityTimeout {
			return fmt.Errorf("visibility_timeout_seconds cannot exceed %d (12 hours)", maxVisibilityTimeout)
		}
	}

	// Long poll timeout: 0 to 20 seconds
	if longPollTimeoutSeconds > 0 {
		maxLongPollTimeout := int64(20)
		if longPollTimeoutSeconds > maxLongPollTimeout {
			return fmt.Errorf("long_poll_timeout_seconds cannot exceed %d (20 seconds)", maxLongPollTimeout)
		}
	}

	return nil
}

// ValidateReceiveRequest validates a receive request
func ValidateReceiveRequest(resourcePath string, maxJobs int32, visibilityTimeoutSeconds int64, longPollTimeoutSeconds int64) error {
	// Resource path validation is done at API layer via BuildResourcePath

	// Max jobs: 1 to 100
	if maxJobs < 1 {
		return fmt.Errorf("max_jobs must be at least 1")
	}
	if maxJobs > 100 {
		return fmt.Errorf("max_jobs cannot exceed 100")
	}

	// Visibility timeout: 1 second to 12 hours
	if visibilityTimeoutSeconds > 0 {
		if visibilityTimeoutSeconds < 1 {
			return fmt.Errorf("visibility_timeout_seconds must be at least 1")
		}
		maxVisibilityTimeout := int64(12 * 60 * 60) // 12 hours
		if visibilityTimeoutSeconds > maxVisibilityTimeout {
			return fmt.Errorf("visibility_timeout_seconds cannot exceed %d (12 hours)", maxVisibilityTimeout)
		}
	}

	// Long poll timeout: 0 to 20 seconds
	if longPollTimeoutSeconds > 0 {
		maxLongPollTimeout := int64(20)
		if longPollTimeoutSeconds > maxLongPollTimeout {
			return fmt.Errorf("long_poll_timeout_seconds cannot exceed %d (20 seconds)", maxLongPollTimeout)
		}
	}

	return nil
}

// ValidateACKRequest validates an ACK request
func ValidateACKRequest(resourcePath string, jobID string) error {
	// Resource path validation is done at API layer via BuildResourcePath

	if jobID == "" {
		return fmt.Errorf("job_id is required")
	}

	return nil
}

// ValidateNACKRequest validates a NACK request
func ValidateNACKRequest(resourcePath string, jobID string, delaySeconds int64) error {
	// Resource path validation is done at API layer via BuildResourcePath

	if jobID == "" {
		return fmt.Errorf("job_id is required")
	}

	if delaySeconds < 0 {
		return fmt.Errorf("delay_seconds cannot be negative")
	}

	// Max delay: 1 year
	maxDelay := int64(365 * 24 * 60 * 60)
	if delaySeconds > maxDelay {
		return fmt.Errorf("delay_seconds cannot exceed %d (1 year)", maxDelay)
	}

	return nil
}

// ValidateGetQueueStatsRequest validates a get queue stats request
func ValidateGetQueueStatsRequest(resourcePath string) error {
	// Resource path validation is done at API layer via BuildResourcePath
	return nil
}

// ValidateVisibilityTimeout validates a visibility timeout duration
func ValidateVisibilityTimeout(timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("visibility timeout must be positive")
	}

	maxTimeout := 12 * time.Hour
	if timeout > maxTimeout {
		return fmt.Errorf("visibility timeout cannot exceed %v", maxTimeout)
	}

	return nil
}

// ValidateDelay validates a delay duration
func ValidateDelay(delay time.Duration) error {
	if delay < 0 {
		return fmt.Errorf("delay cannot be negative")
	}

	maxDelay := 365 * 24 * time.Hour
	if delay > maxDelay {
		return fmt.Errorf("delay cannot exceed %v (1 year)", maxDelay)
	}

	return nil
}

// ValidateMaxJobs validates max jobs for batch receive
func ValidateMaxJobs(maxJobs int) error {
	if maxJobs < 1 {
		return fmt.Errorf("max_jobs must be at least 1")
	}

	if maxJobs > 100 {
		return fmt.Errorf("max_jobs cannot exceed 100")
	}

	return nil
}
