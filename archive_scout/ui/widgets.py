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
    """Install one root-level wheel router and dispatch to the nearest scroll surface.

    Native Text/Listbox/Treeview widgets keep their normal wheel behavior while
    they can still move.  At a boundary the event is handed to the nearest outer
    Archive Scout scroll owner, so nested panes never scroll twice.
    """
    _attr = "_archive_scout_wheel_router"

    def __init__(self, root: tk.Misc) -> None:
        self.root = root
        self._wheel_residual: dict[int, float] = {}
        root.bind_all("<MouseWheel>", self._wheel, add=True)
        root.bind_all("<Shift-MouseWheel>", self._shift_wheel, add=True)
        root.bind_all("<Button-4>", self._wheel, add=True)
        root.bind_all("<Button-5>", self._wheel, add=True)
        root.bind_all("<FocusIn>", self._focus_in, add=True)

    @classmethod
    def ensure(cls, widget: tk.Misc) -> "WheelRouter":
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
    def _view_can_move(widget: tk.Misc, units: int, *, horizontal: bool = False) -> bool:
        if not units:
            return False
        try:
            first, last = (widget.xview() if horizontal else widget.yview())
            eps = 1e-6
            return first > eps if units < 0 else last < 1.0 - eps
        except (tk.TclError, AttributeError, ValueError):
            return False

    def _units(self, event) -> int:
        if getattr(event, "num", None) == 4:
            return -1
        if getattr(event, "num", None) == 5:
            return 1
        delta = float(getattr(event, "delta", 0) or 0)
        if not delta:
            return 0
        # Smooth macOS trackpads emit many tiny deltas, while Windows wheels
        # typically deliver +/-120 per notch. Aggregate fractions rather than
        # turning every touchpad event into a full canvas unit.
        scale = 3.0 if sys.platform == "darwin" else 120.0
        key = id(getattr(event, "widget", self.root))
        accumulated = self._wheel_residual.get(key, 0.0) + (-delta / scale)
        if abs(accumulated) < 1.0:
            self._wheel_residual[key] = accumulated
            return 0
        units = max(-3, min(3, int(accumulated)))
        self._wheel_residual[key] = max(-0.99, min(0.99, accumulated - units))
        return units

    def _wheel(self, event):
        widget = getattr(event, "widget", None)
        units = self._units(event)
        if not units:
            return "break" if self._native_scrollable(widget) else None
        # Native class bindings run before the all-binding.  If the widget can
        # move, simply consume the event here to prevent an outer-page double scroll.
        if self._native_scrollable(widget) and self._view_can_move(widget, units):
            return "break"
        for owner in self._owners(widget):
            if hasattr(owner, "can_scroll_y") and owner.can_scroll_y(units):
                owner.scroll_y(units)
                return "break"
        return "break" if self._native_scrollable(widget) else None

    def _shift_wheel(self, event):
        widget = getattr(event, "widget", None)
        units = self._units(event)
        if not units:
            return "break" if self._native_scrollable(widget) else None
        if self._native_scrollable(widget) and self._view_can_move(widget, units, horizontal=True):
            return "break"
        for owner in self._owners(widget):
            if hasattr(owner, "can_scroll_x") and owner.can_scroll_x(units):
                owner.scroll_x(units)
                return "break"
        return "break" if self._native_scrollable(widget) else None

    def _focus_in(self, event):
        widget = getattr(event, "widget", None)
        for owner in self._owners(widget):
            if isinstance(owner, ScrollablePage):
                owner.after_idle(lambda o=owner, w=widget: o.reveal(w))
                break
        return None


class ScrollablePage(ttk.Frame):
    """Responsive vertical page, with horizontal access to wide controls.

    Respect the width of the viewport when content fits; preserve its requested
    width when it does not. A horizontal scrollbar then exposes the entire
    form instead of silently clipping controls off the right edge.
    """
    def __init__(self, master, *, padding=10, frame_style: str | None = None, **kwargs) -> None:
        super().__init__(master, **kwargs)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self.canvas = tk.Canvas(self, highlightthickness=0, borderwidth=0)
        self.vbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.hbar = ttk.Scrollbar(self, orient="horizontal", command=self.canvas.xview)
        self.canvas.configure(yscrollcommand=self.vbar.set, xscrollcommand=self.hbar.set)
        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.vbar.grid(row=0, column=1, sticky="ns")
        self.hbar.grid(row=1, column=0, sticky="ew")
        self.hbar.grid_remove()
        self.body = ttk.Frame(self.canvas, padding=padding, style=frame_style or "TFrame")
        self._window = self.canvas.create_window((0, 0), window=self.body, anchor="nw")
        self.canvas._archive_scout_scroll_owner = self  # type: ignore[attr-defined]
        self.body._archive_scout_scroll_owner = self  # type: ignore[attr-defined]
        self.body.bind("<Configure>", self._queue_region, add=True)
        self.canvas.bind("<Configure>", self._viewport, add=True)
        self._region_job = None
        WheelRouter.ensure(self)

    def _viewport(self, event=None) -> None:
        self._queue_region()

    def _queue_region(self, _event=None) -> None:
        if self._region_job is not None:
            try:
                self.after_cancel(self._region_job)
            except tk.TclError:
                pass
        self._region_job = self.after_idle(self._update_region)

    def _update_region(self) -> None:
        self._region_job = None
        try:
            width = max(1, self.canvas.winfo_width())
            requested = self.body.winfo_reqwidth()
            content_width = max(width, requested)
            self.canvas.itemconfigure(self._window, width=content_width)
            self.canvas.configure(scrollregion=self.canvas.bbox("all"))
            if content_width > width + 1:
                self.hbar.grid()
            else:
                self.hbar.grid_remove()
                self.canvas.xview_moveto(0)
        except tk.TclError:
            pass

    def can_scroll_y(self, units: int) -> bool:
        return WheelRouter._view_can_move(self.canvas, units)

    def can_scroll_x(self, units: int) -> bool:
        return WheelRouter._view_can_move(self.canvas, units, horizontal=True)

    def scroll_y(self, units: int) -> None:
        self.canvas.yview_scroll(int(units), "units")

    def scroll_x(self, units: int) -> None:
        self.canvas.xview_scroll(int(units), "units")

    def reveal(self, widget: tk.Misc) -> None:
        # Focus changes must not move a control that is already visible.
        try:
            if not widget.winfo_ismapped():
                return
            self.update_idletasks()
            top = widget.winfo_rooty() - self.canvas.winfo_rooty()
            bottom = top + widget.winfo_height()
            available = self.canvas.winfo_height()
            margin = 6
            if top >= margin and bottom <= available - margin:
                return
            scrollheight = max(1, self.body.winfo_height())
            delta = (top - margin) if top < margin else (bottom - available + margin)
            first = self.canvas.yview()[0]
            self.canvas.yview_moveto(max(0.0, min(1.0, first + delta / scrollheight)))
        except (tk.TclError, AttributeError):
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
