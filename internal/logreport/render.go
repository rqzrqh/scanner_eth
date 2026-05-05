package logreport

import (
	"embed"
	"html/template"
	"io"
)

//go:embed templates/report.html
var templateFS embed.FS

func RenderHTML(w io.Writer, report *Report) error {
	tpl, err := template.New("report.html").Funcs(TemplateFuncs()).ParseFS(templateFS, "templates/report.html")
	if err != nil {
		return err
	}
	return tpl.ExecuteTemplate(w, "report.html", report)
}
