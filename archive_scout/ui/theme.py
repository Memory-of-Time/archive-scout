from __future__ import annotations

import os
import sys
import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk

LIGHT = {
    "bg": "#f4f6f8", "panel": "#ffffff", "sidebar": "#172033", "sidebar_hover": "#26344f",
    "sidebar_text": "#f7f9fc", "text": "#18202d", "muted": "#667085", "border": "#d0d5dd",
    "accent": "#2563eb", "accent_active": "#1d4ed8", "success": "#15803d", "warning": "#b45309",
    "danger": "#b42318", "selection": "#dbeafe", "input": "#ffffff",
}
DARK = {
    "bg": "#111827", "panel": "#182235", "sidebar": "#0b1220", "sidebar_hover": "#24324a",
    "sidebar_text": "#f8fafc", "text": "#e5e7eb", "muted": "#9ca3af", "border": "#344054",
    "accent": "#60a5fa", "accent_active": "#93c5fd", "success": "#4ade80", "warning": "#fbbf24",
    "danger": "#f87171", "selection": "#1e3a5f", "input": "#101827",
}
HIGH_CONTRAST = {
    "bg": "#000000", "panel": "#000000", "sidebar": "#000000", "sidebar_hover": "#1a1a1a",
    "sidebar_text": "#ffffff", "text": "#ffffff", "muted": "#d9d9d9", "border": "#ffffff",
    "accent": "#ffff00", "accent_active": "#ffffff", "success": "#00ff00", "warning": "#ffff00",
    "danger": "#ff6666", "selection": "#1f4fff", "input": "#000000",
}
LIGHT_REVIEW_COLORS = {
    "relevant": "#dcfce7", "possibly_relevant": "#fef3c7", "false_positive": "#fee2e2",
    "duplicate": "#e0e7ff", "dead_end": "#e5e7eb", "needs_follow_up": "#ffedd5", "unreviewed": "#f8fafc",
}
DARK_REVIEW_COLORS = {
    "relevant": "#143a2a", "possibly_relevant": "#453817", "false_positive": "#4a2427",
    "duplicate": "#28345c", "dead_end": "#303744", "needs_follow_up": "#4b301b", "unreviewed": "#182235",
}
# Backward-compatible export for callers that still import the old name.
REVIEW_COLORS = LIGHT_REVIEW_COLORS


def enable_windows_dpi_awareness() -> None:
    """Declare DPI awareness before creating the first Tk window.

    Windows should own physical DPI scaling.  Scout's user scale is a
    font preference and must not overwrite Tk's global pixels-per-point value.
    """
    if os.name != "nt":
        return
    try:
        import ctypes
        # PER_MONITOR_AWARE_V2.  Negative pseudo-handles are passed as void*.
        if ctypes.windll.user32.SetProcessDpiAwarenessContext(ctypes.c_void_p(-4)):
            return
    except Exception:
        pass
    try:
        import ctypes
        if ctypes.windll.shcore.SetProcessDpiAwareness(2) in (0, None):
            return
    except Exception:
        pass
    try:
        import ctypes
        ctypes.windll.user32.SetProcessDPIAware()
    except Exception:
        pass


def _windows_high_contrast() -> bool:
    if os.name != "nt":
        return False
    try:
        import ctypes
        from ctypes import wintypes

        class HIGHCONTRASTW(ctypes.Structure):
            _fields_ = [("cbSize", wintypes.UINT), ("dwFlags", wintypes.DWORD), ("lpszDefaultScheme", wintypes.LPWSTR)]

        info = HIGHCONTRASTW()
        info.cbSize = ctypes.sizeof(info)
        SPI_GETHIGHCONTRAST = 0x0042
        HCF_HIGHCONTRASTON = 0x00000001
        ok = ctypes.windll.user32.SystemParametersInfoW(SPI_GETHIGHCONTRAST, info.cbSize, ctypes.byref(info), 0)
        return bool(ok and (info.dwFlags & HCF_HIGHCONTRASTON))
    except Exception:
        return False


def _windows_theme() -> str | None:
    if os.name != "nt":
        return None
    if _windows_high_contrast():
        return "high_contrast"
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize") as key:
            value, _ = winreg.QueryValueEx(key, "AppsUseLightTheme")
            return "light" if int(value) else "dark"
    except Exception:
        return None


def detect_system_theme(root: tk.Misc | None = None) -> str:
    env = os.environ.get("ARCHIVE_SCOUT_THEME", "").strip().casefold()
    if env in {"light", "dark"}:
        return env
    win = _windows_theme()
    if win:
        return win
    if sys.platform == "darwin" and root is not None:
        try:
            value = root.tk.call("exec", "defaults", "read", "-g", "AppleInterfaceStyle")
            if str(value).casefold() == "dark":
                return "dark"
        except Exception:
            pass
    return "light"


def palette_for(root: tk.Misc, requested: str) -> tuple[str, dict[str, str]]:
    name = requested.strip().casefold()
    if name == "system":
        name = detect_system_theme(root)
    if name not in {"light", "dark", "high_contrast"}:
        name = "light"
    if name == "high_contrast":
        return name, HIGH_CONTRAST
    return name, DARK if name == "dark" else LIGHT


def review_colors_for(theme: str) -> dict[str, str]:
    return DARK_REVIEW_COLORS if theme in {"dark", "high_contrast"} else LIGHT_REVIEW_COLORS


def _named_fonts(root: tk.Tk, scale: float) -> dict[str, tkfont.Font]:
    names = ("TkDefaultFont", "TkTextFont", "TkFixedFont", "TkMenuFont", "TkHeadingFont")
    if not hasattr(root, "_archive_scout_font_baseline"):
        baseline: dict[str, dict[str, object]] = {}
        for name in names:
            font = tkfont.nametofont(name, root=root)
            baseline[name] = dict(font.actual())
        root._archive_scout_font_baseline = baseline  # type: ignore[attr-defined]
    baseline = root._archive_scout_font_baseline  # type: ignore[attr-defined]
    result: dict[str, tkfont.Font] = {}
    for name in names:
        font = tkfont.nametofont(name, root=root)
        base = baseline[name]
        size = int(base.get("size", 10) or 10)
        sign = -1 if size < 0 else 1
        scaled = max(7, int(round(abs(size) * scale))) * sign
        font.configure(
            family=base.get("family"), size=scaled, weight=base.get("weight", "normal"),
            slant=base.get("slant", "roman"), underline=base.get("underline", 0), overstrike=base.get("overstrike", 0),
        )
        result[name] = font
    return result


def apply_theme(root: tk.Tk, requested: str = "system", font_scale: float = 1.0) -> tuple[str, dict[str, str]]:
    resolved, colors = palette_for(root, requested)
    scale = min(1.75, max(0.8, float(font_scale)))
    fonts = _named_fonts(root, scale)
    style = ttk.Style(root)
    available = set(style.theme_names())
    preferred = "vista" if os.name == "nt" and resolved in {"light", "high_contrast"} and "vista" in available else "clam"
    try:
        style.theme_use(preferred if preferred in available else style.theme_use())
    except tk.TclError:
        pass

    default = fonts["TkDefaultFont"]
    heading = fonts["TkHeadingFont"]
    accent_text = "#000000" if resolved == "high_contrast" else "#ffffff"
    line_height = max(24, int(default.metrics("linespace") + 9))
    root.configure(background=colors["bg"])
    style.configure(".", background=colors["bg"], foreground=colors["text"], font=default)
    style.configure("TFrame", background=colors["bg"])
    style.configure("Panel.TFrame", background=colors["panel"], relief="flat")
    style.configure("TLabel", background=colors["bg"], foreground=colors["text"])
    style.configure("Panel.TLabel", background=colors["panel"], foreground=colors["text"])
    style.configure("Muted.TLabel", background=colors["bg"], foreground=colors["muted"])
    style.configure("Title.TLabel", background=colors["bg"], foreground=colors["text"], font=(default.actual("family"), max(16, abs(int(default.actual("size"))) + 10), "bold"))
    style.configure("Section.TLabel", background=colors["bg"], foreground=colors["text"], font=(heading.actual("family"), max(10, abs(int(heading.actual("size"))) + 2), "bold"))
    style.configure("CardValue.TLabel", background=colors["panel"], foreground=colors["accent"], font=(default.actual("family"), max(15, abs(int(default.actual("size"))) + 8), "bold"))
    style.configure("CardTitle.TLabel", background=colors["panel"], foreground=colors["muted"])
    style.configure("TLabelFrame", background=colors["bg"], foreground=colors["text"], bordercolor=colors["border"], relief="solid")
    style.configure("TLabelFrame.Label", background=colors["bg"], foreground=colors["text"], font=(default.actual("family"), abs(int(default.actual("size"))), "bold"))
    style.configure("TButton", padding=(10, 6), background=colors["panel"], foreground=colors["text"], bordercolor=colors["border"])
    style.map("TButton", background=[("active", colors["selection"]), ("pressed", colors["selection"])])
    style.configure("Accent.TButton", background=colors["accent"], foreground=accent_text, bordercolor=colors["accent"], font=(default.actual("family"), abs(int(default.actual("size"))), "bold"))
    style.map("Accent.TButton", background=[("active", colors["accent_active"]), ("pressed", colors["accent_active"])])
    style.configure("Danger.TButton", foreground=colors["danger"])
    style.configure("Sidebar.TFrame", background=colors["sidebar"])
    style.configure("Sidebar.TLabel", background=colors["sidebar"], foreground=colors["sidebar_text"])
    style.configure("Sidebar.TButton", background=colors["sidebar"], foreground=colors["sidebar_text"], borderwidth=0, anchor="w", padding=(14, 9))
    style.map("Sidebar.TButton", background=[("active", colors["sidebar_hover"]), ("pressed", colors["sidebar_hover"])])
    style.configure("SidebarActive.TButton", background=colors["accent"], foreground=accent_text, borderwidth=0, anchor="w", padding=(14, 9), font=(default.actual("family"), abs(int(default.actual("size"))), "bold"))
    style.map("SidebarActive.TButton", background=[("active", colors["accent_active"])])
    style.configure("Status.TLabel", background=colors["panel"], foreground=colors["text"], padding=(8, 5))
    # Keep editable fields visibly bounded on every platform/theme. Native Tk
    # themes vary considerably in how much of an Entry/Text border they draw;
    # an explicit one-pixel border prevents large white/dark editor areas from
    # visually blending into their parent frame.
    style.configure(
        "TEntry", fieldbackground=colors["input"], foreground=colors["text"],
        insertcolor=colors["text"], bordercolor=colors["border"], borderwidth=1,
        relief="solid", padding=3,
    )
    style.configure(
        "TCombobox", fieldbackground=colors["input"], foreground=colors["text"],
        background=colors["panel"], arrowcolor=colors["text"], bordercolor=colors["border"],
        borderwidth=1, padding=2,
    )
    style.map("TCombobox", fieldbackground=[("readonly", colors["input"])], foreground=[("readonly", colors["text"])])
    style.configure("Treeview", background=colors["panel"], fieldbackground=colors["panel"], foreground=colors["text"], rowheight=line_height, bordercolor=colors["border"])
    style.map("Treeview", background=[("selected", colors["accent"])], foreground=[("selected", accent_text)])
    style.configure("Treeview.Heading", background=colors["bg"], foreground=colors["text"], font=(heading.actual("family"), abs(int(heading.actual("size"))), "bold"), padding=(6, 6))
    style.configure("TNotebook", background=colors["bg"], borderwidth=0)
    style.configure("TNotebook.Tab", padding=(10, 6))
    style.layout("Sidebar.TNotebook.Tab", [])
    style.configure("Sidebar.TNotebook", background=colors["bg"], borderwidth=0)
    style.configure("Horizontal.TProgressbar", background=colors["accent"], troughcolor=colors["border"], bordercolor=colors["border"])
    return resolved, colors


def apply_text_theme(widget: tk.Misc, colors: dict[str, str]) -> None:
    for child in widget.winfo_children():
        if isinstance(child, (tk.Text, tk.Listbox)):
            try:
                child.configure(
                    background=colors["input"], foreground=colors["text"], insertbackground=colors["text"],
                    selectbackground=colors["accent"], selectforeground="#ffffff",
                    highlightbackground=colors["border"], highlightcolor=colors["accent"],
                    highlightthickness=1, borderwidth=1, relief="solid",
                )
            except tk.TclError:
                pass
        elif isinstance(child, tk.Canvas):
            try:
                child.configure(background=colors["bg"], highlightthickness=0)
            except tk.TclError:
                pass
        apply_text_theme(child, colors)
