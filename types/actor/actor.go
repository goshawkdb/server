package actor

import (
	"goshawkdb.io/common/actor"
)

type EnqueueActor interface {
	actor.EnqueueMsgActor
	actor.EnqueueFuncActor
}
