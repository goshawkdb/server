package debug

import (
	"fmt"
	ui "github.com/jroimartin/gocui"
	"time"
)

type Events struct{}

func (e Events) Layout(g *ui.Gui) error {
	screenWidth, screenHeight := g.Size()
	v, err := g.SetView(EVENTS, 0, screenHeight-EVENTS_HEIGHT, screenWidth-1, screenHeight-1)
	if err != nil {
		if err != ui.ErrUnknownView {
			return err
		}
		v.Wrap = true
		v.Autoscroll = true
		v.Frame = true
		v.Title = "Events"
	}
	return nil
}

func AppendEvent(g *ui.Gui, msg string) error {
	ev, err := g.View(EVENTS)
	if err != nil {
		return err
	}
	start := ""
	if l, _ := ev.Line(0); len(l) > 0 {
		start = "\n"
	}
	fmt.Fprintf(ev, "%s%30.30v: %s", start, time.Now().Format(time.RFC3339Nano), msg)
	return nil
}
