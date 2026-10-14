from __future__ import annotations

import sys
import tkinter as tk
from tkinter import ttk


class ToolTip:
    def __init__(self, widget: tk.Misc, text: str, delay_ms: int = 500) -> None:
        self.widget, self.text, self.delay_ms = widget, text, delay_ms
        self.after_id: str | None = None
        self.window: tk.Toplevel | None = None
        widget.bind("<Enter>", self._schedule, add=True)
        widget.bind("<Leave>", self.hide, add=True)
        widget.bind("<ButtonPress>", self.hide, add=True)
    def _schedule(self, _event=None) -> None:
        self.hide(); self.after_id = self.widget.after(self.delay_ms, self.show)
    def show(self) -> None:
        self.after_id = None
        if self.window or not self.text: return
        x, y = self.widget.winfo_rootx()+18, self.widget.winfo_rooty()+self.widget.winfo_height()+6
        window = tk.Toplevel(self.widget); window.wm_overrideredirect(True); window.wm_geometry(f"+{x}+{y}")
        ttk.Label(window, text=self.text, padding=(8,5), wraplength=420, style="Status.TLabel").pack(); self.window=window
    def hide(self, _event=None) -> None:
        if self.after_id:
            try: self.widget.after_cancel(self.after_id)
            except tk.TclError: pass
            self.after_id=None
        if self.window: self.window.destroy(); self.window=None


class WheelRouter:
    """Install one interpreter-wide wheel router and dispatch each event once.

    Tk native Text/Listbox/Treeview class bindings run before ``bind_all``.  The
    router therefore remembers the native view observed after the previous event:
    if the view changed on this event, the native widget consumed it; only a later
    event that arrives while the same native surface is already stationary at its
    boundary may bubble to an outer Archive Scout scroll owner.
    """
    _attr = "_archive_scout_wheel_router"

    def __init__(self, root: tk.Misc) -> None:
        self.root = root
        self._wheel_residual: dict[tuple[int, str], float] = {}
        self._native_views: dict[tuple[int, str], tuple[float, float]] = {}
        root.bind_all("<MouseWheel>", self._wheel, add=True)
        root.bind_all("<Shift-MouseWheel>", self._shift_wheel, add=True)
        root.bind_all("<Button-4>", self._wheel, add=True)
        root.bind_all("<Button-5>", self._wheel, add=True)
        root.bind_all("<FocusIn>", self._focus_in, add=True)

    @classmethod
    def ensure(cls, widget: tk.Misc) -> "WheelRouter":
        # ``bind_all`` is interpreter-wide, not toplevel-local.  Store exactly
        # one router on the Tk interpreter root so repeatedly opening dialogs
        # cannot stack duplicate global handlers.
        try:
            root = widget._root()  # type: ignore[attr-defined]
        except Exception:
            root = widget.winfo_toplevel()
        router = getattr(root, cls._attr, None)
        if router is None:
            router = cls(root)
            setattr(root, cls._attr, router)
        return router

    @staticmethod
    def _owners(widget: tk.Misc | None):
        current = widget
        seen: set[int] = set()
        while current is not None:
            owner = getattr(current, "_archive_scout_scroll_owner", None)
            if owner is not None and id(owner) not in seen:
                seen.add(id(owner))
                yield owner
            try:
                current = current.master
            except Exception:
                break

    @staticmethod
    def _native_scrollable(widget: tk.Misc | None) -> bool:
        return isinstance(widget, (tk.Text, tk.Listbox, ttk.Treeview))

    @staticmethod
    def _view(widget: tk.Misc, *, horizontal: bool = False) -> tuple[float, float] | None:
        try:
            first, last = (widget.xview() if horizontal else widget.yview())
            return float(first), float(last)
        except (tk.TclError, AttributeError, TypeError, ValueError):
            return None

    @staticmethod
    def _view_can_move_from(view: tuple[float, float] | None, units: int) -> bool:
        if not units or view is None:
            return False
        first, last = view
        eps = 1e-6
        return first > eps if units < 0 else last < 1.0 - eps

    def _pointer_widget(self, event):
        try:
            x_root = int(getattr(event, "x_root"))
            y_root = int(getattr(event, "y_root"))
            containing = self.root.winfo_containing(x_root, y_root)
            if containing is not None:
                return containing
        except (AttributeError, TypeError, ValueError, tk.TclError):
            pass
        return getattr(event, "widget", None)

    def _units(self, event, surface: object, *, horizontal: bool = False) -> int:
        if getattr(event, "num", None) == 4:
            return -1
        if getattr(event, "num", None) == 5:
            return 1
        delta = float(getattr(event, "delta", 0) or 0)
        if not delta:
            return 0
        # Tk normally reports Windows/X11 wheel notches in multiples of 120,
        # while native macOS trackpads typically provide small/high-resolution
        # deltas.  Some macOS Tk builds and synthetic/test events can still
        # deliver the classic +/-120 form, so normalize that shape as one
        # logical notch instead of treating it as 120 scroll units.
        if sys.platform == "darwin":
            quotient = delta / 120.0
            classic_notch = abs(delta) >= 120.0 and abs(quotient - round(quotient)) < 1e-9
            divisor = 120.0 if classic_notch else 1.0
        else:
            divisor = 120.0
        key = (id(surface), "x" if horizontal else "y")
        accumulated = self._wheel_residual.get(key, 0.0) + (-delta / divisor)
        if abs(accumulated) < 1.0:
            self._wheel_residual[key] = accumulated
            return 0
        units = int(accumulated)
        self._wheel_residual[key] = accumulated - units
        return units

    def _native_consumed(self, widget: tk.Misc, units: int, *, horizontal: bool = False) -> bool:
        """Return whether the native class binding consumed this wheel event."""
        axis = "x" if horizontal else "y"
        key = (id(widget), axis)
        current = self._view(widget, horizontal=horizontal)
        previous = self._native_views.get(key)
        if current is not None:
            self._native_views[key] = current
        if current is None:
            return True
        # If movement remains possible after the class binding, keep the event
        # on the native surface.  If it just arrived at a boundary, the changed
        # view proves this same event was already consumed there too.
        if self._view_can_move_from(current, units):
            return True
        if previous is None:
            return True
        moved = abs(current[0] - previous[0]) > 1e-9 or abs(current[1] - previous[1]) > 1e-9
        return moved

    def _route(self, event, *, horizontal: bool = False):
        widget = self._pointer_widget(event)
        owners = list(self._owners(widget))
        surface = widget if self._native_scrollable(widget) else (owners[0] if owners else widget or self.root)
        units = self._units(event, surface, horizontal=horizontal)
        if not units:
            return "break" if self._native_scrollable(widget) else None

        if self._native_scrollable(widget) and self._native_consumed(widget, units, horizontal=horizontal):
            return "break"

        method_can = "can_scroll_x" if horizontal else "can_scroll_y"
        method_scroll = "scroll_x" if horizontal else "scroll_y"
        for owner in owners:
            can_scroll = getattr(owner, method_can, None)
            scroll = getattr(owner, method_scroll, None)
            if callable(can_scroll) and callable(scroll) and can_scroll(units):
                scroll(units)
                return "break"
        return "break" if self._native_scrollable(widget) else None

    def _wheel(self, event):
        return self._route(event, horizontal=False)

    def _shift_wheel(self, event):
        return self._route(event, horizontal=True)

    def _focus_in(self, event):
        widget = getattr(event, "widget", None)
        for owner in self._owners(widget):
            if isinstance(owner, ScrollablePage):
                owner.request_reveal(widget)
                break
        return None


class ScrollablePage(ttk.Frame):
    """A responsive page body with a persistent vertical scrollbar.

    Footer/status bars live outside this widget.  The canvas window always
    follows viewport width, while content height expands naturally.
    """
    def __init__(self, master, *, padding=10, frame_style: str | None = None, horizontal=False, **kwargs) -> None:
        super().__init__(master, **kwargs)
        self._horizontal = bool(horizontal)
        self.columnconfigure(0, weight=1); self.rowconfigure(0, weight=1)
        self.canvas = tk.Canvas(self, highlightthickness=0, borderwidth=0)
        self.vbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.vbar.set)
        self.canvas.grid(row=0, column=0, sticky="nsew"); self.vbar.grid(row=0, column=1, sticky="ns")
        if self._horizontal:
            self.hbar = ttk.Scrollbar(self, orient="horizontal", command=self.canvas.xview)
            self.hbar.grid(row=1, column=0, sticky="ew")
            ttk.Frame(self).grid(row=1, column=1, sticky="nsew")
            self.canvas.configure(xscrollcommand=self.hbar.set)
        self.body = ttk.Frame(self.canvas, padding=padding, style=frame_style or "TFrame")
        self._window = self.canvas.create_window((0,0), window=self.body, anchor="nw")
        self.canvas._archive_scout_scroll_owner = self  # type: ignore[attr-defined]
        self.body._archive_scout_scroll_owner = self  # type: ignore[attr-defined]
        self.body.bind("<Configure>", self._queue_region, add=True)
        self.canvas.bind("<Configure>", self._viewport, add=True)
        WheelRouter.ensure(self)
        self._region_job = None
        self._reveal_job = None
        self._reveal_target: tk.Misc | None = None

    def _viewport(self, event=None) -> None:
        width = max(1, int(getattr(event, "width", self.canvas.winfo_width())))
        if getattr(self, "_horizontal", False):
            width = max(width, self.body.winfo_reqwidth())
        self.canvas.itemconfigure(self._window, width=width); self._queue_region()

    def _queue_region(self, _event=None) -> None:
        if self._region_job is not None:
            try: self.after_cancel(self._region_job)
            except tk.TclError: pass
        self._region_job = self.after_idle(self._update_region)

    def _update_region(self) -> None:
        self._region_job = None
        if getattr(self, "_horizontal", False):
            self.canvas.itemconfigure(self._window, width=max(1, self.canvas.winfo_width(), self.body.winfo_reqwidth()))
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def can_scroll_y(self, units: int) -> bool:
        try:
            first, last = self.canvas.yview()
            return first > 1e-6 if units < 0 else last < 1.0 - 1e-6
        except tk.TclError:
            return False

    def can_scroll_x(self, units: int) -> bool:
        if not getattr(self, "_horizontal", False):
            return False
        try:
            first, last = self.canvas.xview()
            return first > 1e-6 if units < 0 else last < 1.0 - 1e-6
        except tk.TclError:
            return False
    def scroll_y(self, units: int) -> None: self.canvas.yview_scroll(int(units), "units")
    def scroll_x(self, units: int) -> None:
        if getattr(self, "_horizontal", False):
            self.canvas.xview_scroll(int(units), "units")

    def request_reveal(self, widget: tk.Misc | None) -> None:
        if widget is None:
            return
        self._reveal_target = widget
        if self._reveal_job is not None:
            return
        self._reveal_job = self.after_idle(self._run_reveal)

    def _run_reveal(self) -> None:
        self._reveal_job = None
        widget = self._reveal_target
        self._reveal_target = None
        if widget is None:
            return
        try:
            if hasattr(widget, "winfo_exists") and not widget.winfo_exists():
                return
            if hasattr(widget, "winfo_ismapped") and not widget.winfo_ismapped():
                return
            focus_get = getattr(self, "focus_get", None)
            if callable(focus_get):
                focused = focus_get()
                if focused is not None and focused is not widget:
                    return
            if not any(owner is self for owner in WheelRouter._owners(widget)):
                return
            self.reveal(widget)
        except (tk.TclError, AttributeError):
            return

    def _reveal_margin(self) -> int:
        try:
            pixels_per_point = float(self.canvas.winfo_fpixels("1p"))
            return max(6, min(24, int(round(6.0 * pixels_per_point))))
        except (tk.TclError, AttributeError, TypeError, ValueError):
            return 8

    def reveal(self, widget: tk.Misc) -> None:
        """Reveal only an off-screen focused widget, using minimum movement."""
        if getattr(self, "_horizontal", False):
            try:
                left = self.canvas.winfo_rootx() + self._reveal_margin()
                right = self.canvas.winfo_rootx() + self.canvas.winfo_width() - self._reveal_margin()
                widget_left = widget.winfo_rootx()
                widget_right = widget_left + widget.winfo_width()
                delta = widget_left - left if widget_left < left else max(0, widget_right - right)
                bounds = self.canvas.bbox("all")
                if delta and bounds:
                    span = max(1, bounds[2] - bounds[0])
                    self.canvas.xview_moveto(max(0.0, min(1.0, self.canvas.xview()[0] + delta / span)))
            except (tk.TclError, AttributeError, TypeError):
                pass
        try:
            viewport_top = int(self.canvas.winfo_rooty())
            viewport_height = max(1, int(self.canvas.winfo_height()))
            viewport_bottom = viewport_top + viewport_height
            widget_top = int(widget.winfo_rooty())
            widget_height = max(1, int(widget.winfo_height()))
            widget_bottom = widget_top + widget_height
            margin = min(self._reveal_margin(), max(0, viewport_height // 4))
            visible_top = viewport_top + margin
            visible_bottom = viewport_bottom - margin

            if widget_height <= max(1, visible_bottom - visible_top):
                if widget_top >= visible_top and widget_bottom <= visible_bottom:
                    return
                delta = widget_top - visible_top if widget_top < visible_top else widget_bottom - visible_bottom
            else:
                # For a control taller than the viewport, anchor its top once;
                # alternating top/bottom corrections would oscillate forever.
                if abs(widget_top - visible_top) <= 1:
                    return
                delta = widget_top - visible_top

            try:
                bbox = self.canvas.bbox("all")
            except (tk.TclError, AttributeError):
                bbox = None
            if bbox:
                content_height = max(viewport_height, int(bbox[3]) - int(bbox[1]))
            else:
                content_height = max(viewport_height, int(self.body.winfo_reqheight()))
            first, _last = self.canvas.yview()
            max_first = max(0.0, 1.0 - (viewport_height / max(1.0, float(content_height))))
            target = max(0.0, min(max_first, float(first) + (float(delta) / max(1.0, float(content_height)))))
            if abs(target - float(first)) > 1e-9:
                self.canvas.yview_moveto(target)
        except (tk.TclError, AttributeError, TypeError, ValueError):
            pass


class ScrollableTree(ttk.Frame):
    def __init__(self, master, **tree_kwargs) -> None:
        super().__init__(master)
        self.columnconfigure(0, weight=1); self.rowconfigure(0, weight=1)
        self.tree = ttk.Treeview(self, **tree_kwargs)
        self.vbar = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.hbar = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=self.vbar.set, xscrollcommand=self.hbar.set)
        self.tree.grid(row=0,column=0,sticky="nsew"); self.vbar.grid(row=0,column=1,sticky="ns"); self.hbar.grid(row=1,column=0,sticky="ew")
        self.tree._archive_scout_scroll_owner = self  # type: ignore[attr-defined]
        WheelRouter.ensure(self)
    def can_scroll_y(self, units: int) -> bool:
        try:
            first, last = self.tree.yview()
            return first > 1e-6 if units < 0 else last < 1.0 - 1e-6
        except tk.TclError:
            return False
    def can_scroll_x(self, units: int) -> bool:
        try:
            first, last = self.tree.xview()
            return first > 1e-6 if units < 0 else last < 1.0 - 1e-6
        except tk.TclError:
            return False
    def scroll_y(self, units: int) -> None: self.tree.yview_scroll(int(units), "units")
    def scroll_x(self, units: int) -> None: self.tree.xview_scroll(int(units), "units")


class CollapsibleFrame(ttk.Frame):
    def __init__(self, master, text: str, initially_open: bool=False, **kwargs) -> None:
        super().__init__(master, **kwargs); self.open_var=tk.BooleanVar(value=initially_open)
        self.button=ttk.Checkbutton(self,text=text,variable=self.open_var,command=self._toggle,style="Toolbutton"); self.button.grid(row=0,column=0,sticky="w")
        self.body=ttk.Frame(self); self.body.grid(row=1,column=0,sticky="nsew",pady=(6,0)); self.columnconfigure(0,weight=1); self._toggle()
    def _toggle(self) -> None:
        self.body.grid() if self.open_var.get() else self.body.grid_remove()
