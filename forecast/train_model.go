package forecast

import (
	"context"
	"errors"
)

func trainModel(ctx context.Context, model Algorithm, start, end int) error {

	switch m := model.(type) {
	case *ExponentialSmoothing:
		var (
			α, st, Yorigin float64
		)

		α = m.alpha

		// Training smoothing Level
		for i := start; i < end+1; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}

			xt := m.data[i]

			if i == start {
				st = xt
				m.training.initialLevel = xt

			} else if i == end { // Setting the last value in traindata as Yorigin value for bootstrapping
				Yorigin = m.data[i]
				m.training.originValue = Yorigin
			} else {
				st = α*xt + (1-α)*st
			}
		}
		m.training.smoothingLevel = st

	// case *HoltWinters:

	default:
		return errors.New("Unsupported Model passed")

	}

	return nil
}
