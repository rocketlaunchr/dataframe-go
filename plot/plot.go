package plot

import (
	"encoding/base64"
	"errors"
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"text/template"

	"github.com/zserge/lorca"

	dataframe "github.com/rocketlaunchr/dataframe-go"
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
	imgHTML   = `<html><head><title>{{.Title}}</title></head><body><img src="{{.Src}}" height="100%" width="100%"></img><body></html>`

	htmlTemplate = template.Must(template.New("github.com/rocketlaunchr/dataframe-go/plot/html").Parse(titleHTML))
	imgTemplate  = template.Must(template.New("github.com/rocketlaunchr/dataframe-go/plot/img").Parse(imgHTML))
)

type injectData struct {
	Title string
	Src   string
}

// Plot represents a plot window.
type Plot struct {

	// The channel that indicates the plot window has been closed.
	// The window can be closed by the user or the Close function.
	Closed chan struct{}

	title    string
	tempfile *os.File

	ui lorca.UI
}

// Open creates a new plot window.
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
//  graph.Render(chart.PNG, plt)
//  plt.Display()
//  <-plt.Closed
//
func Open(title string, width, height int) (*Plot, error) {

	if lorca.LocateChrome() == "" {
		return nil, ErrNoChromeInstalled
	}

	// Create a temporary file
	tmpfile, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, err
	}

	// Build html string
	ij := injectData{Title: title}

	builder := &strings.Builder{}

	err = htmlTemplate.Execute(builder, ij)
	if err != nil {
		return nil, err
	}

	ui, err := lorca.New("data:text/html,"+url.PathEscape(builder.String()), "", width, height)
	if err != nil {
		return nil, err
	}

	plot := &Plot{
		ui:       ui,
		title:    title,
		tempfile: tmpfile,
		Closed:   make(chan struct{}, 1),
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

	err := dataframe.NewErrorCollection()

	errA := p.tempfile.Close()
	if errA != nil {
		err.AddError(errA, false)
	}
	errB := os.Remove(p.tempfile.Name()) // Delete out temporary file
	if errB != nil {
		err.AddError(errB, false)
	}
	p.tempfile = nil
	p.title = ""

	errC := p.ui.Close()
	if errC != nil {
		err.AddError(errC, false)
	}

	if !err.IsNil(false) {
		return err
	}

	p.ui = nil

	return nil
}

// Write implements io.Writer interface. Do not use this method.
// Any plotting package that writes to an io.Writer (such as to file) is compatible.
//
// See: https://godoc.org/github.com/wcharczuk/go-chart
func (p *Plot) Write(d []byte) (int, error) {

	if p.ui == nil {
		return 0, ErrClosed
	}

	return p.tempfile.Write(d)
}

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

// Display will display the plot.
func (p *Plot) Display(mime ...MIME) error {

	if p.ui == nil {
		return ErrClosed
	}

	img, err := ioutil.ReadFile(p.tempfile.Name())
	if err != nil {
		return err
	}

	var prefix string
	if len(mime) == 0 {
		prefix = "data:image/png;base64, "
	} else {
		prefix = "data:image/" + string(mime[0]) + ";base64, "
	}
	b64Img := prefix + base64.StdEncoding.EncodeToString(img)

	// Build html string
	ij := injectData{Title: p.title, Src: b64Img}

	builder := &strings.Builder{}

	err = imgTemplate.Execute(builder, ij)
	if err != nil {
		return err
	}

	return p.ui.Load("data:text/html," + url.PathEscape(builder.String()))
}
