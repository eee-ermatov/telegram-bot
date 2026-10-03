"""Рекламный motion-design ролик про Telegram (15 сек, 1280x720, 30 fps, со звуком).
Кадры рисуются через Pillow, звук синтезируется numpy, всё склеивает ffmpeg.
Запуск: python media/telegram_motion.py  -> media/telegram_how_it_works.mp4
Зависимости: pillow, numpy, ffmpeg.
"""
import math
import os
import random
import subprocess
import tempfile
import wave

import numpy as np
from PIL import Image, ImageDraw, ImageFilter, ImageFont

W, H, S = 1280, 720, 2          # S — суперсэмплинг для сглаживания
FPS, DUR = 30, 15.0
SR = 44100
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "telegram_how_it_works.mp4")

NAVY1, NAVY2 = (8, 14, 30), (16, 34, 66)
BLUE = (42, 171, 238)
BLUE2 = (34, 120, 220)
CYAN = (110, 220, 255)
WHITE = (255, 255, 255)
GREY = (150, 168, 192)
DARK = (22, 30, 46)
PANEL = (30, 40, 60)

FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
FONT_B = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
_fonts = {}


def f(size, bold=True):
    key = (int(size * S), bold)
    if key not in _fonts:
        _fonts[key] = ImageFont.truetype(FONT_B if bold else FONT, key[0])
    return _fonts[key]


# ---------- easing ----------
def clamp(x, a=0.0, b=1.0):
    return max(a, min(b, x))


def seg(t, a, b):
    return clamp((t - a) / (b - a))


def ease(t):
    t = clamp(t)
    return 4 * t ** 3 if t < 0.5 else 1 - (-2 * t + 2) ** 3 / 2


def out_expo(t):
    t = clamp(t)
    return 1 if t == 1 else 1 - 2 ** (-10 * t)


def in_expo(t):
    t = clamp(t)
    return 0 if t == 0 else 2 ** (10 * t - 10)


def back(t, c=1.9):
    t = clamp(t)
    return 1 + (c + 1) * (t - 1) ** 3 + c * (t - 1) ** 2


def mix(a, b, k):
    return tuple(int(a[i] + (b[i] - a[i]) * k) for i in range(len(a)))


def P(*v):
    return [x * S for x in v]


def rgba(c, a):
    return (*c[:3], int(255 * clamp(a)))


# ---------- статичные заготовки ----------
def make_bg():
    img = Image.new("RGB", (W * S, H * S))
    d = ImageDraw.Draw(img)
    for y in range(H * S):
        d.line([(0, y), (W * S, y)], fill=mix(NAVY1, NAVY2, y / (H * S)))
    return img


def make_glow(r, color):
    g = Image.new("RGBA", (r * 2 * S, r * 2 * S), (0, 0, 0, 0))
    ImageDraw.Draw(g).ellipse((r * S * 0.5, r * S * 0.5, r * S * 1.5, r * S * 1.5), fill=(*color, 150))
    return g.filter(ImageFilter.GaussianBlur(r * S * 0.28))


BG = make_bg()
GLOW_B = make_glow(260, BLUE)
GLOW_C = make_glow(200, (120, 80, 255))
GLOW_S = make_glow(60, CYAN)

random.seed(7)
STARS = [(random.uniform(0, W), random.uniform(0, H), random.uniform(0.6, 2.2), random.uniform(0.2, 1)) for _ in range(90)]


def paste_glow(img, g, cx, cy, a=1.0):
    if a <= 0.01:
        return
    if a < 1:
        g = g.copy()
        g.putalpha(g.getchannel("A").point(lambda v: int(v * a)))
    img.alpha_composite(g, (int(cx * S - g.width / 2), int(cy * S - g.height / 2)))


# ---------- примитивы ----------
def draw_text(d, xy, s, fnt, color, a=1.0):
    """Текст с настоящей прозрачностью (ImageDraw игнорирует альфу шрифта)."""
    if a <= 0.01:
        return
    if a >= 0.99:
        d.text(xy, s, font=fnt, fill=rgba(color, 1))
        return
    l, t_, r, b = d.textbbox((0, 0), s, font=fnt)
    layer = Image.new("RGBA", (r + 4, b + 4), (0, 0, 0, 0))
    ImageDraw.Draw(layer).text((0, 0), s, font=fnt, fill=rgba(color, 1))
    layer.putalpha(layer.getchannel("A").point(lambda v: int(v * a)))
    d._image.alpha_composite(layer, (int(xy[0]), int(xy[1])))


def text_c(d, cx, cy, s, size, color, a=1.0, bold=True):
    fnt = f(size, bold)
    tw = d.textlength(s, font=fnt) / S
    draw_text(d, P(cx - tw / 2, cy - size * 0.62), s, fnt, color, a)
    return tw


def plane(d, x, y, ang, size, color=WHITE, a=1.0):
    pts = [(1.0, 0), (-0.75, -0.62), (-0.32, 0.02), (-0.75, 0.62)]
    ca, sa = math.cos(ang), math.sin(ang)
    poly = [((x + size * (px * ca - py * sa)) * S, (y + size * (px * sa + py * ca)) * S) for px, py in pts]
    d.polygon(poly, fill=rgba(color, a))
    fold = [poly[0], poly[2], ((x + size * (-0.45 * ca - 0.3 * sa)) * S, (y + size * (-0.45 * sa + 0.3 * ca)) * S)]
    d.polygon(fold, fill=rgba(mix(color, BLUE2, 0.35), a))


def logo(d, cx, cy, r, a=1.0, spin=0.0):
    d.ellipse(P(cx - r, cy - r, cx + r, cy + r), fill=rgba(BLUE, a))
    d.ellipse(P(cx - r * 0.92, cy - r * 0.92, cx + r * 0.92, cy + r * 0.92), fill=rgba(mix(BLUE, CYAN, 0.25), a))
    plane(d, cx - r * 0.06, cy + r * 0.04, -0.35 + spin, r * 0.55, WHITE, a)


def bubble(d, x, y, s, k, out=True, size=15, checks=0, a=1.0):
    if k <= 0.01:
        return
    fnt = f(size * k, False)
    tw = d.textlength(s, font=fnt) / S
    pad = 13 * k
    bw = tw + pad * 2 + (24 * k if checks else 0)
    bh = size * 2.2 * k
    x0 = x - bw if out else x
    d.rounded_rectangle(P(x0, y, x0 + bw, y + bh), radius=bh * 0.45 * S,
                        fill=rgba(BLUE if out else PANEL, a))
    draw_text(d, P(x0 + pad, y + bh / 2 - size * 0.62 * k), s, fnt, WHITE, a)
    if checks:
        cx, cy = x0 + bw - 24 * k, y + bh / 2
        for i in range(checks):
            o = i * 6 * k
            d.line(P(cx + o, cy, cx + 4 * k + o, cy + 4 * k, cx + 12 * k + o, cy - 6 * k),
                   fill=rgba(WHITE, a), width=max(1, int(2 * S * k)))


def phone(d, cx, cy, k=1.0, title="", a=1.0):
    w, h = 230 * k, 440 * k
    x0, y0 = cx - w / 2, cy - h / 2
    d.rounded_rectangle(P(x0, y0, x0 + w, y0 + h), radius=36 * k * S, fill=rgba((44, 54, 76), a))
    d.rounded_rectangle(P(x0 + 9 * k, y0 + 9 * k, x0 + w - 9 * k, y0 + h - 9 * k), radius=28 * k * S, fill=rgba(DARK, a))
    d.rounded_rectangle(P(cx - 34 * k, y0 + 18 * k, cx + 34 * k, y0 + 30 * k), radius=6 * k * S, fill=rgba((0, 0, 0), a))
    d.rectangle(P(x0 + 9 * k, y0 + 42 * k, x0 + w - 9 * k, y0 + 88 * k), fill=rgba(PANEL, a))
    d.ellipse(P(x0 + 22 * k, y0 + 50 * k, x0 + 52 * k, y0 + 80 * k), fill=rgba((255, 140, 90), a))
    if title:
        draw_text(d, P(x0 + 62 * k, y0 + 55 * k), title, f(15 * k), WHITE, a)
        draw_text(d, P(x0 + 62 * k, y0 + 72 * k), "в сети", f(10 * k, False), CYAN, a)
    d.rounded_rectangle(P(x0 + 20 * k, y0 + h - 64 * k, x0 + w - 62 * k, y0 + h - 28 * k), radius=18 * k * S, fill=rgba(PANEL, a))
    return x0, y0, w, h


def laptop(d, cx, cy, k=1.0, a=1.0):
    w, h = 300 * k, 190 * k
    x0, y0 = cx - w / 2, cy - h / 2
    d.rounded_rectangle(P(x0, y0, x0 + w, y0 + h), radius=12 * k * S, fill=rgba((44, 54, 76), a))
    d.rectangle(P(x0 + 10 * k, y0 + 10 * k, x0 + w - 10 * k, y0 + h - 10 * k), fill=rgba(DARK, a))
    d.rectangle(P(x0 + 10 * k, y0 + 10 * k, x0 + 85 * k, y0 + h - 10 * k), fill=rgba(PANEL, a))
    for i in range(4):
        yy = y0 + 22 * k + i * 38 * k
        d.ellipse(P(x0 + 18 * k, yy, x0 + 42 * k, yy + 24 * k), fill=rgba(mix(BLUE, (255, 140, 90), i / 3), a))
        d.rounded_rectangle(P(x0 + 48 * k, yy + 8 * k, x0 + 78 * k, yy + 15 * k), radius=3 * k * S, fill=rgba(GREY, a * 0.6))
    d.polygon(P(x0 - 24 * k, y0 + h, x0 + w + 24 * k, y0 + h, x0 + w + 6 * k, y0 + h + 14 * k, x0 - 6 * k, y0 + h + 14 * k),
              fill=rgba((60, 72, 98), a))
    return x0, y0, w, h


def tablet(d, cx, cy, k=1.0, a=1.0):
    w, h = 210 * k, 280 * k
    x0, y0 = cx - w / 2, cy - h / 2
    d.rounded_rectangle(P(x0, y0, x0 + w, y0 + h), radius=20 * k * S, fill=rgba((44, 54, 76), a))
    d.rounded_rectangle(P(x0 + 10 * k, y0 + 10 * k, x0 + w - 10 * k, y0 + h - 10 * k), radius=12 * k * S, fill=rgba(DARK, a))
    d.rectangle(P(x0 + 10 * k, y0 + 22 * k, x0 + w - 10 * k, y0 + 56 * k), fill=rgba(PANEL, a))
    return x0, y0, w, h


def icon(d, kind, cx, cy, r, a=1.0):
    d.ellipse(P(cx - r, cy - r, cx + r, cy + r), fill=rgba(WHITE, a * 0.12))
    d.ellipse(P(cx - r * 0.82, cy - r * 0.82, cx + r * 0.82, cy + r * 0.82), fill=rgba(WHITE, a))
    c = rgba(BLUE2, a)
    if kind == "bolt":
        d.polygon(P(cx + 0.1 * r, cy - 0.6 * r, cx - 0.38 * r, cy + 0.08 * r, cx - 0.02 * r, cy + 0.08 * r,
                    cx - 0.12 * r, cy + 0.6 * r, cx + 0.38 * r, cy - 0.1 * r, cx + 0.02 * r, cy - 0.1 * r), fill=c)
    elif kind == "lock":
        d.arc(P(cx - 0.26 * r, cy - 0.58 * r, cx + 0.26 * r, cy), 180, 360, fill=c, width=int(0.12 * r * S))
        d.line(P(cx - 0.26 * r, cy - 0.3 * r, cx - 0.26 * r, cy - 0.05 * r), fill=c, width=int(0.12 * r * S))
        d.line(P(cx + 0.26 * r, cy - 0.3 * r, cx + 0.26 * r, cy - 0.05 * r), fill=c, width=int(0.12 * r * S))
        d.rounded_rectangle(P(cx - 0.42 * r, cy - 0.1 * r, cx + 0.42 * r, cy + 0.48 * r), radius=0.1 * r * S, fill=c)
        d.ellipse(P(cx - 0.08 * r, cy + 0.08 * r, cx + 0.08 * r, cy + 0.24 * r), fill=rgba(WHITE, a))
    elif kind == "cloud":
        for dx, dy, rr in [(-0.25, 0.08, 0.24), (0.05, -0.1, 0.32), (0.3, 0.1, 0.22)]:
            d.ellipse(P(cx + (dx - rr) * r, cy + (dy - rr) * r, cx + (dx + rr) * r, cy + (dy + rr) * r), fill=c)
        d.rectangle(P(cx - 0.25 * r, cy + 0.08 * r, cx + 0.3 * r, cy + 0.32 * r), fill=c)
    elif kind == "bot":
        d.line(P(cx, cy - 0.55 * r, cx, cy - 0.32 * r), fill=c, width=int(0.08 * r * S))
        d.ellipse(P(cx - 0.08 * r, cy - 0.66 * r, cx + 0.08 * r, cy - 0.5 * r), fill=c)
        d.rounded_rectangle(P(cx - 0.45 * r, cy - 0.32 * r, cx + 0.45 * r, cy + 0.4 * r), radius=0.18 * r * S, fill=c)
        for sx in (-0.18, 0.18):
            d.ellipse(P(cx + (sx - 0.09) * r, cy - 0.09 * r, cx + (sx + 0.09) * r, cy + 0.09 * r), fill=rgba(WHITE, a))
        d.line(P(cx - 0.15 * r, cy + 0.22 * r, cx + 0.15 * r, cy + 0.22 * r), fill=rgba(WHITE, a), width=int(0.06 * r * S))


def bezier(p0, p1, p2, t):
    u = 1 - t
    return (u * u * p0[0] + 2 * u * t * p1[0] + t * t * p2[0],
            u * u * p0[1] + 2 * u * t * p1[1] + t * t * p2[1])


def trail(d, p0, p1, p2, k, n=22, width=1.0):
    for i in range(n):
        kk = clamp(k - i * 0.012)
        x, y = bezier(p0, p1, p2, kk)
        r = (7 - i * 0.28) * width
        if r > 0:
            d.ellipse(P(x - r, y - r, x + r, y + r), fill=rgba(mix(CYAN, BLUE2, i / n), 1 - i / n))


def ring(d, cx, cy, r, w, a):
    if a > 0.01 and r > 0:
        d.ellipse(P(cx - r, cy - r, cx + r, cy + r), outline=rgba(CYAN, a), width=max(1, int(w * S)))


def flight(d, t, t0, t1, p0, p1, p2, size=26):
    if not (t0 <= t <= t1):
        return None
    k = ease(seg(t, t0, t1))
    trail(d, p0, p1, p2, k)
    x, y = bezier(p0, p1, p2, k)
    x2, y2 = bezier(p0, p1, p2, clamp(k + 0.01))
    plane(d, x, y, math.atan2(y2 - y, x2 - x), size)
    return x, y


# ---------- сцены ----------
def scene_hook(d, t):
    """0–2.4: кинетическая типографика."""
    out = ease(seg(t, 2.0, 2.4))
    a = 1 - out
    k1 = out_expo(seg(t, 0.15, 0.45))
    if k1 > 0:
        sc = 1.8 - 0.8 * k1
        text_c(d, W / 2, 300 - 40 * out, "Одно сообщение.", 64 * sc, WHITE, a * k1)
    k2 = out_expo(seg(t, 0.95, 1.25))
    if k2 > 0:
        sc = 1.8 - 0.8 * k2
        text_c(d, W / 2, 400 + 40 * out, "Весь мир на связи.", 52 * sc, CYAN, a * k2)
    # полоса-подчёркивание
    lk = ease(seg(t, 1.25, 1.7))
    if lk > 0 and a > 0.01:
        d.rounded_rectangle(P(W / 2 - 220 * lk, 448, W / 2 + 220 * lk, 454), radius=3 * S, fill=rgba(BLUE, a))


def scene_send(d, t):
    """2.3–5.4: телефон, набор, отправка."""
    if t > 5.4:
        return
    k = back(seg(t, 2.3, 2.85), 1.4)
    cy = 360 + (1 - k) * 600
    ex = ease(seg(t, 4.9, 5.4))
    cx = 640 - 260 * ex
    a = 1 - ease(seg(t, 5.1, 5.4))
    x0, y0, w, h = phone(d, cx, cy, 1.0, "Аня", a)
    msg = "Встречаемся в 8?"
    n = int(len(msg) * seg(t, 2.9, 3.9))
    sent = t >= 4.1
    if not sent and n:
        draw_text(d, P(x0 + 32, y0 + h - 55), msg[:n] + ("|" if int(t * 6) % 2 else ""), f(13, False), WHITE, a)
    # кнопка отправки
    pk = math.sin(math.pi * seg(t, 3.95, 4.25))
    r = 18 * (1 + 0.35 * pk)
    sx, sy = x0 + w - 40, y0 + h - 46
    d.ellipse(P(sx - r, sy - r, sx + r, sy + r), fill=rgba(BLUE, a))
    plane(d, sx, sy, 0, 10, WHITE, a)
    ring(d, sx, sy, 18 + 40 * seg(t, 4.0, 4.4), 3, (1 - seg(t, 4.0, 4.4)) * a)
    # входящее и исходящее сообщения
    bubble(d, x0 + 22, y0 + 108, "Привет! Ты где? 🙂".replace(" 🙂", ""), back(seg(t, 2.7, 2.95)), out=False, a=a)
    checks = 1 if t > 4.6 else 0
    bubble(d, x0 + w - 22, y0 + 158, msg, back(seg(t, 4.1, 4.35)), out=True, checks=checks, a=a)
    # самолётик вылетает из телефона
    flight(d, t, 4.25, 5.4, (sx, sy), (cx + 200, 120), (1400, 260), 28)
    if t < 4.1:
        text_c(d, 640, 64, "Напиши.", 34, WHITE, ease(seg(t, 2.6, 2.9)))
    else:
        text_c(d, 640, 64, "Отправь.", 34, WHITE, ease(seg(t, 4.1, 4.3)) * (1 - seg(t, 5.0, 5.3)))


def globe(d, cx, cy, r, t, a):
    d.ellipse(P(cx - r, cy - r, cx + r, cy + r), fill=rgba((20, 46, 90), a * 0.8), outline=rgba(BLUE, a), width=2 * S)
    for i in range(6):
        ph = (t * 0.6 + i / 6) % 1
        rx = r * abs(math.cos(ph * math.pi))
        d.ellipse(P(cx - rx, cy - r, cx + rx, cy + r), outline=rgba(BLUE, a * 0.45), width=S)
    for la in (-0.6, -0.3, 0, 0.3, 0.6):
        yy = cy + la * r
        rx = r * math.sqrt(1 - la * la)
        d.line(P(cx - rx, yy, cx + rx, yy), fill=rgba(BLUE, a * 0.35), width=S)


def scene_sync(d, img, t):
    """5.3–8.8: глобус и мгновенная доставка на все устройства."""
    if not (5.3 <= t <= 8.8):
        return
    a_in = ease(seg(t, 5.3, 5.7))
    a = a_in * (1 - ease(seg(t, 8.45, 8.8)))
    gk = back(seg(t, 5.3, 5.9), 1.2)
    G = (640, 380)
    paste_glow(img, GLOW_B, *G, 0.7 * a)
    globe(d, *G, 120 * gk, t, a)
    # самолётик облетает глобус
    if 5.4 <= t <= 6.4:
        u = ease(seg(t, 5.4, 6.4))
        ang = math.pi + u * math.pi * 1.4
        x, y = G[0] + 170 * math.cos(ang), G[1] + 70 * math.sin(ang)
        for i in range(16):
            aa = ang - i * 0.05
            px, py = G[0] + 170 * math.cos(aa), G[1] + 70 * math.sin(aa)
            rr = 6 - i * 0.35
            d.ellipse(P(px - rr, py - rr, px + rr, py + rr), fill=rgba(CYAN, a * (1 - i / 16)))
        plane(d, x, y, ang + math.pi / 2, 24, WHITE, a)
    # устройства и лучи
    devs = [("phone", 190, 400, 6.3), ("laptop", 1080, 270, 6.65), ("tablet", 1080, 560, 7.0)]
    for kind, dx, dy, td in devs:
        dk = back(seg(t, td - 0.35, td), 1.5)
        if dk <= 0:
            continue
        if kind == "phone":
            x0, y0, w, h = phone(d, dx, dy, 0.62 * dk, "", a)
            bx, by, out = x0 + 14, y0 + 75, False
        elif kind == "laptop":
            x0, y0, w, h = laptop(d, dx, dy, 0.8 * dk, a)
            bx, by, out = x0 + 80, y0 + 30, False
        else:
            x0, y0, w, h = tablet(d, dx, dy, 0.75 * dk, a)
            bx, by, out = x0 + 16, y0 + 60, False
        # луч от глобуса
        lk = ease(seg(t, td - 0.3, td))
        ex, ey = G[0] + (dx - G[0]) * lk, G[1] + (dy - G[1]) * lk
        d.line(P(G[0], G[1], ex, ey), fill=rgba(CYAN, a * 0.5 * (1 - seg(t, td + 0.2, td + 0.8))), width=2 * S)
        d.ellipse(P(ex - 6, ey - 6, ex + 6, ey + 6), fill=rgba(WHITE, a * (1 - seg(t, td, td + 0.3))))
        bubble(d, bx, by, "Встречаемся в 8?", back(seg(t, td, td + 0.25)) * 0.8, out=out, size=13, a=a)
        ring(d, dx, dy, 40 + 120 * seg(t, td, td + 0.5), 3, a * (1 - seg(t, td, td + 0.5)))
    text_c(d, 640, 64, "Мгновенно. На всех устройствах.", 34, WHITE, a * ease(seg(t, 5.5, 5.9)))


FEATURES = [
    ("bolt", "Быстро", "Сообщения доходят за доли секунды", (34, 120, 220)),
    ("lock", "Защищено", "Шифрование и секретные чаты", (90, 70, 210)),
    ("cloud", "Облако", "Файлы до 2 ГБ, доступ отовсюду", (20, 150, 190)),
    ("bot", "Каналы и боты", "Автоматизируй что угодно", (40, 170, 140)),
]
F0, FD = 8.7, 0.8


def scene_features(d, img, t):
    """8.7–11.9: быстрые карточки-преимущества."""
    if not (F0 <= t <= F0 + FD * 4 + 0.1):
        return
    i = min(3, int((t - F0) / FD))
    lt = t - F0 - i * FD
    kind, title, sub, col = FEATURES[i]
    # цветная заливка-переход, въезжающая слева направо
    wipe = out_expo(clamp(lt / 0.3))
    prev = FEATURES[i - 1][3] if i > 0 else NAVY2
    d.rectangle(P(0, 0, W, H), fill=rgba(prev, 1 if i > 0 else wipe * 0.0))
    d.rectangle(P(0, 0, W * wipe, H), fill=rgba(col, 1))
    if i == 3:
        d.rectangle(P(0, 0, W, H), fill=rgba(NAVY1, ease(seg(t, F0 + FD * 4 - 0.2, F0 + FD * 4 + 0.1))))
    k = out_expo(seg(lt, 0.08, 0.4))
    a = 1 - ease(seg(lt, FD - 0.12, FD))
    if i == 3 and t > F0 + FD * 4 - 0.2:
        a = 0
    shift = (1 - k) * 160
    icon(d, kind, 360 + shift * 0.5, 360, 90 * back(seg(lt, 0.05, 0.35)), a)
    draw_text(d, P(500 + shift, 290), title, f(64), WHITE, a * k)
    draw_text(d, P(504 + shift * 1.5, 385), sub, f(26, False), WHITE, a * k * 0.85)
    # счётчик
    for j in range(4):
        cx = 560 + j * 26
        d.ellipse(P(cx - 5, 640 - 5, cx + 5, 640 + 5), fill=rgba(WHITE, (1 if j == i else 0.35) * a))


def scene_final(d, img, t):
    """11.8–15: сообщения слетаются в логотип, слоган."""
    if t < 11.8:
        return
    C = (640, 300)
    # пузыри-сообщения стягиваются к центру
    random.seed(3)
    msgs = ["Привет!", "Скоро буду", "Фото 📷".replace(" 📷", ""), "Ок 👍".replace(" 👍", ""), "Созвон?", "Готово",
            "Люблю ❤".replace(" ❤", ""), "Едем!", "Ха-ха", "Спасибо"]
    for j, m in enumerate(msgs):
        ang = j / len(msgs) * math.tau + 0.3
        st = 11.8 + j * 0.04
        u = in_expo(seg(t, st, st + 0.75))
        R = 520 * (1 - u) + 10
        bx, by = C[0] + R * math.cos(ang), C[1] + R * 0.62 * math.sin(ang)
        bubble(d, bx - 40, by - 14, m, (1 - u) * 0.9 + 0.1, out=j % 2 == 0, size=14,
               a=ease(seg(t, st, st + 0.15)) * (1 - seg(t, st + 0.65, st + 0.75)))
    # вспышка и логотип
    hit = 12.65
    if t >= hit:
        paste_glow(img, GLOW_B, *C, 0.9)
        paste_glow(img, GLOW_C, C[0] + 120, C[1] + 60, 0.4)
        flash = 1 - seg(t, hit, hit + 0.35)
        lk = back(seg(t, hit, hit + 0.45), 2.2)
        for n in range(3):
            rt = seg(t, hit + n * 0.12, hit + 0.9 + n * 0.12)
            ring(d, *C, 80 + 420 * out_expo(rt), 4 - n, (1 - rt) * 0.8)
        logo(d, *C, 86 * lk, 1.0, spin=0.15 * math.sin(t * 2))
        if flash > 0:
            d.rectangle(P(0, 0, W, H), fill=rgba(WHITE, flash * 0.55))
        tk = out_expo(seg(t, 13.0, 13.5))
        if tk > 0:
            text_c(d, 640, 470 + 30 * (1 - tk), "Telegram", 72, WHITE, tk)
        for n, word in enumerate(["Просто.", "Быстро.", "Бесплатно."]):
            wk = out_expo(seg(t, 13.5 + n * 0.22, 13.85 + n * 0.22))
            if wk > 0:
                text_c(d, 470 + n * 180, 560 + 20 * (1 - wk), word, 30, CYAN if n == 2 else GREY, wk)


def frame(t):
    img = BG.copy().convert("RGBA")
    # подвижный фон: свечения и звёзды
    paste_glow(img, GLOW_B, 260 + 120 * math.sin(t * 0.7), 200 + 60 * math.cos(t * 0.5), 0.35)
    paste_glow(img, GLOW_C, 1020 + 100 * math.cos(t * 0.6), 560 + 50 * math.sin(t * 0.8), 0.3)
    d = ImageDraw.Draw(img, "RGBA")
    for x, y, r, b in STARS:
        xx = (x - t * 25 * r) % W
        d.ellipse(P(xx - r, y - r, xx + r, y + r), fill=(160, 210, 255, int(120 * b)))

    scene_hook(d, t)
    scene_send(d, t)
    scene_sync(d, img, t)
    d = ImageDraw.Draw(img, "RGBA")
    scene_features(d, img, t)
    scene_final(d, img, t)

    # камера: лёгкий зум и тряска на ударах
    zoom = 1 + 0.03 * math.sin(t * 0.9)
    shake = 0
    for hit in (0.15, 0.95, 4.0, 12.65):
        shake += 7 * max(0, 1 - (t - hit) / 0.25) if t >= hit else 0
    if t > 12.65:
        zoom += 0.08 * (1 - out_expo(seg(t, 12.65, 13.4)))
    cw, ch = W * S / zoom, H * S / zoom
    ox = (W * S - cw) / 2 + random.uniform(-shake, shake) * S
    oy = (H * S - ch) / 2 + random.uniform(-shake, shake) * S
    img = img.crop((int(ox), int(oy), int(ox + cw), int(oy + ch)))
    return img.convert("RGB").resize((W, H), Image.LANCZOS)


# ---------- звук ----------
def audio():
    n = int(SR * DUR)
    out = np.zeros(n)
    rng = np.random.default_rng(1)

    def add(sig, t, gain=1.0):
        i = int(t * SR)
        j = min(n, i + len(sig))
        if i < n:
            out[i:j] += sig[: j - i] * gain

    def env(length, attack, decay):
        tt = np.arange(int(length * SR)) / SR
        return np.minimum(1, tt / max(attack, 1e-4)) * np.exp(-tt / decay), tt

    def kick():
        e, tt = env(0.35, 0.002, 0.12)
        freq = 45 + 110 * np.exp(-tt * 30)
        return np.sin(2 * np.pi * np.cumsum(freq) / SR) * e

    def hat():
        e, _ = env(0.06, 0.001, 0.015)
        x = rng.standard_normal(len(e))
        return np.diff(x, prepend=0) * e * 0.25

    def whoosh(length=0.6, up=True):
        tt = np.arange(int(length * SR)) / SR
        x = rng.standard_normal(len(tt))
        # плавающий однополюсный фильтр
        y = np.zeros_like(x)
        cut = np.linspace(0.02, 0.35, len(tt)) if up else np.linspace(0.35, 0.02, len(tt))
        acc = 0.0
        for i in range(len(x)):
            acc += cut[i] * (x[i] - acc)
            y[i] = acc
        return y * np.sin(np.pi * tt / length) ** 2 * 1.4

    def pop(f0=900, f1=1500, length=0.09):
        e, tt = env(length, 0.002, length / 3)
        fr = np.linspace(f0, f1, len(tt))
        return np.sin(2 * np.pi * np.cumsum(fr) / SR) * e

    def tick():
        e, _ = env(0.03, 0.0005, 0.006)
        return rng.standard_normal(len(e)) * e * 0.35

    def chime(freqs, length=2.2):
        e, tt = env(length, 0.005, 0.7)
        s = sum(np.sin(2 * np.pi * fr * tt) + 0.3 * np.sin(4 * np.pi * fr * tt) for fr in freqs)
        return s * e / len(freqs)

    def bass(fr, length):
        e, tt = env(length, 0.01, length)
        return np.tanh(2 * np.sin(2 * np.pi * fr * tt)) * e

    # бит 120 bpm
    beat = 0.5
    bars = [55, 55, 65.4, 49]  # A, A, C, G
    for i in range(int(DUR / beat)):
        tb = i * beat
        if 12.2 < tb < 12.65:          # пауза перед финальным ударом
            continue
        if tb < 2.3 and i % 2:          # в начале реже
            continue
        add(kick(), tb, 0.9)
        add(hat(), tb + beat / 2, 0.8)
        if tb >= 2.0 and tb < 12.2:
            add(bass(bars[(i // 4) % 4], beat * 0.9), tb, 0.22)

    # удары на словах
    add(whoosh(0.3), 0.0, 0.4)
    add(pop(300, 120, 0.25), 0.15, 0.5)
    add(whoosh(0.3), 0.8, 0.4)
    add(pop(300, 120, 0.25), 0.95, 0.5)
    add(whoosh(0.5, False), 2.25, 0.5)        # телефон въезжает
    add(pop(700, 1000), 2.75, 0.35)            # входящее
    for k in range(16):                         # набор текста
        add(tick(), 2.92 + k * 0.062, 0.8)
    add(pop(800, 1600, 0.12), 4.05, 0.5)        # отправка
    add(whoosh(1.0), 4.2, 0.7)                  # самолётик улетает
    add(whoosh(0.9, False), 5.35, 0.45)         # облёт глобуса
    for td, fr in ((6.3, 1320), (6.65, 1480), (7.0, 1760)):
        add(pop(fr, fr * 1.2, 0.12), td, 0.45)  # доставки
    for i in range(4):                           # карточки
        tf = F0 + i * FD
        add(whoosh(0.35), tf - 0.05, 0.45)
        add(pop(500 + i * 120, 900 + i * 150, 0.1), tf + 0.1, 0.4)
    add(whoosh(0.85), 11.8, 0.7)                 # всасывание к центру
    add(kick(), 12.65, 1.3)                     # финальный удар
    add(pop(200, 60, 0.6), 12.65, 0.8)
    add(chime([523.25, 659.25, 783.99, 1046.5]), 12.65, 0.5)
    add(chime([587.33, 739.99, 880.0]), 13.5, 0.25)

    # мягкий выход
    fade = np.ones(n)
    fl = int(0.6 * SR)
    fade[-fl:] = np.linspace(1, 0, fl)
    out *= fade
    out /= max(1e-9, np.max(np.abs(out))) / 0.89
    return (out * 32767).astype(np.int16)


def main():
    with tempfile.TemporaryDirectory() as tmp:
        wav = os.path.join(tmp, "a.wav")
        with wave.open(wav, "wb") as w:
            w.setnchannels(1)
            w.setsampwidth(2)
            w.setframerate(SR)
            w.writeframes(audio().tobytes())
        for i in range(int(FPS * DUR)):
            random.seed(1000 + i)
            frame(i / FPS).save(os.path.join(tmp, f"{i:04d}.png"))
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-framerate", str(FPS),
                        "-i", os.path.join(tmp, "%04d.png"), "-i", wav,
                        "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "18",
                        "-c:a", "aac", "-b:a", "192k", "-shortest", OUT], check=True)
    print(OUT)


if __name__ == "__main__":
    main()
