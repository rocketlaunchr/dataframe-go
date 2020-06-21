// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

// Package plot implements basic plotting functionality that leverages Chrome/Chromium for cross-platform
// capabilities. It works on Windows, macOS and Linux.
//
// Any plotting package that can write to an io.Writer is compatible.
//
// See: https://github.com/wcharczuk/go-chart and https://github.com/gonum/plot/wiki/Drawing-to-an-Image-or-Writer:-How-to-save-a-plot-to-an-image.Image-or-an-io.Writer,-not-a-file.#writing-a-plot-to-an-iowriter
package plot

import (
	"bytes"
	"encoding/base64"
	"errors"
	"net/url"
	"strings"
	"text/template"

	"github.com/zserge/lorca"
)

var (
	// ErrNoChromeInstalled indicates that chrome is not installed.
	// Chrome is required to display plots.
	ErrNoChromeInstalled = errors.New("no chrome installed")

	// ErrClosed means the plot window has already been closed
	ErrClosed = errors.New("plot closed")
)

var (
	// Loader: http://jsfiddle.net/kz2nm800/
	titleHTML = `<html><head><title>{{.Title}}</title><style>.loading{position:fixed;top:0;right:0;bottom:0;left:0;background:#fff}.loader{left:50%;margin-left:-4em;font-size:10px;border:.8em solid #dadbdf;border-left:.8em solid #3aa6a5;animation:spin 1.1s infinite linear}.loader,.loader:after{border-radius:50%;width:8em;height:8em;display:block;position:absolute;top:50%;margin-top:-4.05em}@keyframes spin{0%{transform:rotate(360deg)}100%{transform:rotate(0)}}</style></head><body><div class="loading"><div class="loader"></div></div></body></html>`
	imgHTML   = `<html><head><title>{{.Title}}</title></head><body onresize="onresize()"><img id="img" src="{{.Src}}" {{if not .Vertical}}{{if not .Horizontal}}style="height:100%;width:100%;"{{end}}{{end}}></img><script>
		var img = document.getElementById('img');var margin = 8;
		function onresize() {
			{{if not .Vertical}}
				{{if .Horizontal}}
					img.style.height = '100%';
					if (img.width <= document.body.clientWidth-2*margin) {
						img.style.width = '100%';
					} else {
						img.style.removeProperty('width');
					}
				{{end}}
			{{else}}
				{{if not .Horizontal}}
					img.style.width = '100%';
					if (img.height <= document.body.clientHeight-2*margin) {
						img.style.height = '100%';
					} else {
						img.style.removeProperty('height');
					}
				{{end}}
			{{end}}
		}
		document.addEventListener("DOMContentLoaded", onresize);
	</script><body></html>`

	htmlTemplate = template.Must(template.New("github.com/rocketlaunchr/dataframe-go/plot/html").Parse(titleHTML))
	imgTemplate  = template.Must(template.New("github.com/rocketlaunchr/dataframe-go/plot/img").Parse(imgHTML))
)

type injectData struct {
	Title string
	Src   string
	// scroll bars
	Vertical   bool
	Horizontal bool
}

// Plot represents a plot window.
type Plot struct {
	// The channel that indicates the plot window has been closed.
	// The window can be closed by the user or the Close function.
	Closed chan struct{}

	title    string
	tempData bytes.Buffer

	ui lorca.UI
}

// Open creates a new plot window.
// Any plotting package that writes to an io.Writer (such as to file) is compatible.
// Optional wrappers for various plotting packages are provided in the subpackages.
//
// Example:
//
//  import chart "github.com/wcharczuk/go-chart"
//
//  graph := chart.Chart{
//     Series: []chart.Series{
//        chart.TimeSeries{
//           XValues: []time.Time{
//              time.Now().AddDate(0, 0, -2),
//              time.Now().AddDate(0, 0, -1),
//              time.Now(),
//           }
//           YValues: []float64{9.0, 10.0, 11.0},
//        },
//     },
//  }
//
//  plt, _ := plot.Open("Linear", 150, 250)
//  graph.Render(chart.SVG, plt)
//  plt.Display()
//  <-plt.Closed
//
func Open(title string, width, height int) (*Plot, error) {

	if lorca.LocateChrome() == "" {
		return nil, ErrNoChromeInstalled
	}

	// Build html string
	ij := injectData{Title: title}

	builder := &strings.Builder{}

	err := htmlTemplate.Execute(builder, ij)
	if err != nil {
		return nil, err
	}

	ui, err := lorca.New("data:text/html,"+url.PathEscape(builder.String()), "", width, height)
	if err != nil {
		return nil, err
	}

	plot := &Plot{
		ui:     ui,
		title:  title,
		Closed: make(chan struct{}, 1),
	}

	go func() {
		plot.Closed <- <-ui.Done() // triggered when window is destroyed
		ui.Close()
	}()

	return plot, nil
}

// Close closes the plot window.
func (p *Plot) Close() error {
	if p.ui == nil {
		return nil
	}

	p.tempData.Reset()
	p.title = ""

	err := p.ui.Close()
	if err != nil {
		return err
	}

	p.ui = nil

	return nil
}

// Write implements the io.Writer interface. Do not use this method directly.
// Any plotting package that writes to an io.Writer (such as to file) is compatible.
func (p *Plot) Write(d []byte) (int, error) {

	if p.ui == nil {
		return 0, ErrClosed
	}

	return p.tempData.Write(d)
}

// Scrollbar is used to set whether the plot will scroll in the horizontal and/or vertical direction.
type Scrollbar uint8

func (opt Scrollbar) has(x Scrollbar) bool {
	return opt&x != 0
}

// None means that no scrollbar will be added.
// The plot's dimensions will be changed to fit into the the window.
const None Scrollbar = 0
const (
	// Horizontal will add a horizontal scrollbar.
	Horizontal Scrollbar = 1 << iota

	// Vertical will add a vertical scrollbar.
	Vertical
)

// MIME Type is used to help Chrome recognize the format of the image.
type MIME string

const (
	// JPEG MIME Type
	JPEG MIME = "jpeg"

	// PNG MIME Type
	PNG MIME = "png"

	// SVG MIME Type
	SVG MIME = "svg+xml"
)

// Display will display the plot. The default mime is SVG.
// The default scrollbar option is none. This means the entire plot will be made to fit inside
// the window without requiring scrolling.
// If the plotting package you are using supports saving as SVG, then use it.
func (p *Plot) Display(scrollbar Scrollbar, mime ...MIME) error {

	if p.ui == nil {
		return ErrClosed
	}

	var prefix string
	if len(mime) == 0 {
		prefix = "data:image/svg+xml;base64, "
	} else {
		prefix = "data:image/" + string(mime[0]) + ";base64, "
	}
	b64Img := prefix + base64.StdEncoding.EncodeToString(p.tempData.Bytes())

	// Build html string
	ij := injectData{Title: p.title, Src: b64Img}

	if scrollbar.has(Horizontal) {
		ij.Horizontal = true
	}

	if scrollbar.has(Vertical) {
		ij.Vertical = true
	}

	builder := &strings.Builder{}

	err := imgTemplate.Execute(builder, ij)
	if err != nil {
		return err
	}

	return p.ui.Load("data:text/html," + url.PathEscape(builder.String()))
}
