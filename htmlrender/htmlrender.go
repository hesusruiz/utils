package htmlrender

import (
	"bytes"
	"embed"
	"io/fs"
	"log/slog"
	"net/http"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/template/html/v2"
	"github.com/hesusruiz/utils/errl"
)

type Renderer struct {
	engine *html.Engine
}

// NewRenderer creates a new HTML renderer.
// It supports both embedded templates (in viewsfs) and external templates (in extDir).
// If extDir is specified, it exists and it is a directory, it will be used as the directory containing the external templates.
// If extDir is specified, it exists but it is not a directory, we return an error.
// If extDir is not specified, or does not exist, the embedded templates will be used.
func NewRenderer(templateDebug bool, viewsfs *embed.FS, embeddedDir string, externalDir string, extension string) (*Renderer, error) {

	// We need at least one of the two to be provided
	if viewsfs == nil && externalDir == "" {
		return nil, errl.Errorf("viewsfs or externalDir must be provided")
	}

	engine := &html.Engine{}

	if externalDir != "" {
		// externalDir was specified by the user, let's try to use it
		fi, err := os.Stat(externalDir)
		if err == nil {
			// It exists, let's check if it's a directory
			if !fi.IsDir() {
				return nil, errl.Errorf("externalDir must be a directory")
			}

			// externalDir exists and it's a directory, let's use it
			slog.Info("Using external HTML templates")
			engine = html.NewFileSystem(http.Dir(externalDir), extension)
			engine.Reload(templateDebug)

			if err := engine.Load(); err != nil {
				return nil, errl.Error(err)
			}

			renderer := &Renderer{
				engine: engine,
			}

			return renderer, nil
		}
	}

	// We tried with external templates, but we failed, let's try with embedded templates
	if viewsfs == nil {
		return nil, errl.Errorf("viewsfs must be provided")
	}

	viewsDir, err := fs.Sub(*viewsfs, embeddedDir)
	if err != nil {
		return nil, errl.Error(err)
	}

	slog.Info("Using embedded HTML templates")
	engine = html.NewFileSystem(http.FS(viewsDir), extension)
	engine.Reload(false)

	if err := engine.Load(); err != nil {
		return nil, errl.Error(err)
	}

	renderer := &Renderer{
		engine: engine,
	}

	return renderer, nil
}

func (h *Renderer) Render(w http.ResponseWriter, templateName string, data map[string]any, layout ...string) error {

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	ResponseSecurityHeadersStd(w)

	out := &bytes.Buffer{}

	if err := h.engine.Render(out, templateName, data, layout...); err != nil {
		slog.Error("Error rendering template",
			slog.String("error", err.Error()),
		)
		return fiber.NewError(fiber.StatusInternalServerError, "rendering response")
	}

	w.Write(out.Bytes())
	return nil

}

func (h *Renderer) RenderFiber(c *fiber.Ctx, templateName string, data map[string]any, layout ...string) error {

	c.Set("Content-Type", "text/html; charset=utf-8")
	ResponseSecurityHeadersFiber(c)

	out := &bytes.Buffer{}

	if err := h.engine.Render(out, templateName, data, layout...); err != nil {
		slog.Error("Error rendering template",
			slog.String("error", err.Error()),
		)
		return fiber.NewError(fiber.StatusInternalServerError, "rendering response")
	}

	c.Send(out.Bytes())
	return nil

}

// ResponseSecurityHeadersFiber sets the security headers for the response according to best practices
func ResponseSecurityHeadersFiber(c *fiber.Ctx) {

	c.Set("Content-Security-Policy", "frame-ancestors 'none';")
	c.Set("X-Frame-Options", "DENY")
	c.Set("X-Content-Type-Options", "nosniff")
	c.Set("Referrer-Policy", "strict-origin-when-cross-origin")
	c.Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
	c.Set("Cross-Origin-Opener-Policy", "same-origin")
	c.Set("Cross-Origin-Embedder-Policy", "require-corp")
	c.Set("Cross-Origin-Resource-Policy", "same-site")
	c.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), interest-cohort=()")
	c.Set("X-Powered-By", "webserver")

}

func ResponseSecurityHeadersStd(w http.ResponseWriter) {

	w.Header().Set("Content-Security-Policy", "frame-ancestors 'none';")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
	w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
	w.Header().Set("Cross-Origin-Opener-Policy", "same-origin")
	w.Header().Set("Cross-Origin-Embedder-Policy", "require-corp")
	w.Header().Set("Cross-Origin-Resource-Policy", "same-site")
	w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), interest-cohort=()")
	w.Header().Set("X-Powered-By", "webserver")

}
