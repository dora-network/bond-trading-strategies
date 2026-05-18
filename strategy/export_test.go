package strategy

import "github.com/google/uuid"

func RunExists(svc Service, id uuid.UUID) bool {
	s, _ := svc.(*service)
	if s == nil {
		return false
	}
	s.mu.RLock()
	_, ok := s.strategies[id]
	s.mu.RUnlock()
	return ok
}
