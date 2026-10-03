"""Motion-design ролик «Как работает Telegram» (5 сек, 1280x720, 30 fps).
Кадры рисуются через Pillow, склеиваются ffmpeg.
Запуск: python media/telegram_motion.py  -> media/telegram_how_it_works.mp4
"""
import math
import os
import subprocess
import tempfile

from PIL import Image, ImageDraw, ImageFont

W, H, S = 1280, 720, 2          # S — суперсэмплинг для сглаживания
FPS, DUR = 30, 5.0
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "telegram_how_it_works.mp4")

BG1, BG2 = (14, 22, 38), (24, 44, 74)
BLUE = (42, 171, 238)
WHITE = (255, 255, 255)
GREY = (150, 165, 185)
PHONE = (32, 40, 56)
SCREEN = (18, 26, 40)
GREEN = (88, 214, 141)

FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
FONT_B = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"


def f(size, bold=False):
    return ImageFont.truetype(FONT_B if bold else FONT, int(size * S))


def clamp(x, a=0.0, b=1.0):
    return max(a, min(b, x))


def ease(t):  # easeInOutCubic
    t = clamp(t)
    return 4 * t ** 3 if t < 0.5 else 1 - (-2 * t + 2) ** 3 / 2


def back(t):  # easeOutBack — «пружинка»
    t = clamp(t)
    c = 1.7
    return 1 + (c + 1) * (t - 1) ** 3 + c * (t - 1) ** 2


def seg(t, a, b):
    return clamp((t - a) / (b - a))


def P(*v):
    return [x * S for x in v]


def background():
    img = Image.new("RGB", (W * S, H * S))
    d = ImageDraw.Draw(img)
    for y in range(H * S):
        k = y / (H * S)
        d.line([(0, y), (W * S, y)], fill=tuple(int(BG1[i] + (BG2[i] - BG1[i]) * k) for i in range(3)))
    return img


BG = background()


def phone(d, cx, cy, title):
    w, h = 220, 420
    x0, y0 = cx - w / 2, cy - h / 2
    d.rounded_rectangle(P(x0, y0, x0 + w, y0 + h), radius=34 * S, fill=PHONE)
    d.rounded_rectangle(P(x0 + 10, y0 + 10, x0 + w - 10, y0 + h - 10), radius=26 * S, fill=SCREEN)
    d.rounded_rectangle(P(cx - 30, y0 + 18, cx + 30, y0 + 26), radius=4 * S, fill=PHONE)
    # шапка чата
    d.rectangle(P(x0 + 10, y0 + 38, x0 + w - 10, y0 + 80), fill=(28, 38, 56))
    d.ellipse(P(x0 + 22, y0 + 46, x0 + 48, y0 + 72), fill=BLUE)
    d.text(P(x0 + 58, y0 + 50), title, font=f(15, True), fill=WHITE)
    # поле ввода
    d.rounded_rectangle(P(x0 + 20, y0 + h - 62, x0 + w - 60, y0 + h - 28), radius=17 * S, fill=(36, 48, 68))
    return x0, y0, w, h


def bubble(d, x1, y, text, scale, out=True, checks=0):
    if scale <= 0.01:
        return
    fnt = f(14 * scale)
    tw = d.textlength(text, font=fnt) / S
    bw, bh = tw + 26 * scale + (22 * scale if checks else 0), 34 * scale
    x0 = x1 - bw if out else x1
    d.rounded_rectangle(P(x0, y, x0 + bw, y + bh), radius=14 * S * scale,
                        fill=BLUE if out else (44, 58, 82))
    d.text(P(x0 + 13 * scale, y + 8 * scale), text, font=fnt, fill=WHITE)
    if checks:
        cx = x0 + bw - 20 * scale
        for i in range(checks):
            o = i * 5 * scale
            d.line(P(cx + o, y + 19 * scale, cx + 4 * scale + o, y + 23 * scale,
                     cx + 11 * scale + o, y + 13 * scale), fill=WHITE, width=int(2 * S))


def plane(d, x, y, ang, size):
    pts = [(1.0, 0), (-0.7, -0.6), (-0.35, 0), (-0.7, 0.6)]
    ca, sa = math.cos(ang), math.sin(ang)
    poly = [((x + size * (px * ca - py * sa)) * S, (y + size * (px * sa + py * ca)) * S) for px, py in pts]
    d.polygon(poly, fill=WHITE)
    d.line([poly[0], poly[2]], fill=BLUE, width=int(2 * S))


def cloud(d, cx, cy, k, glow):
    if k <= 0.01:
        return
    r = 1.0 * k
    col = tuple(int(c) for c in (40 + 30 * glow, 70 + 60 * glow, 110 + 80 * glow))
    for dx, dy, rr in [(-55, 10, 40), (0, -15, 55), (55, 10, 40), (0, 20, 42)]:
        d.ellipse(P(cx + (dx - rr) * r, cy + (dy - rr) * r, cx + (dx + rr) * r, cy + (dy + rr) * r), fill=col)
    # замок — шифрование
    lw, lh = 34 * r, 26 * r
    d.arc(P(cx - 11 * r, cy - 22 * r, cx + 11 * r, cy + 2 * r), 180, 360, fill=WHITE, width=int(4 * S * r))
    d.rounded_rectangle(P(cx - lw / 2, cy - 8 * r, cx + lw / 2, cy - 8 * r + lh), radius=5 * S * r, fill=WHITE)
    d.ellipse(P(cx - 4 * r, cy + 1 * r, cx + 4 * r, cy + 9 * r), fill=col)


def bezier(p0, p1, p2, t):
    u = 1 - t
    return (u * u * p0[0] + 2 * u * t * p1[0] + t * t * p2[0],
            u * u * p0[1] + 2 * u * t * p1[1] + t * t * p2[1])


def caption(d, text, alpha_k):
    if alpha_k <= 0:
        return
    fnt = f(26, True)
    tw = d.textlength(text, font=fnt) / S
    c = tuple(int(BG2[i] + (WHITE[i] - BG2[i]) * alpha_k) for i in range(3))
    d.text(P(W / 2 - tw / 2, 70 - 10 * (1 - alpha_k)), text, font=fnt, fill=c)


def frame(t):
    img = BG.copy()
    d = ImageDraw.Draw(img)
    A, B, C = (250, 400), (1030, 400), (640, 330)

    # фазы: 0–1 набор, 1–1.5 отправка, 1.5–3.6 полёт, 3.6–5 доставка
    if t < 1.5:
        caption(d, "1. Вы пишете сообщение", ease(seg(t, 0.1, 0.5)))
    elif t < 3.6:
        caption(d, "2. Облако Telegram шифрует и передаёт", ease(seg(t, 1.6, 2.0)))
    else:
        caption(d, "3. Собеседник получает мгновенно", ease(seg(t, 3.7, 4.1)))

    ax0, ay0, aw, ah = phone(d, *A, "Вы")
    bx0, by0, bw, bh = phone(d, *B, "Друг")

    full = "Привет, как дела?"
    # набор текста в поле ввода
    typed = full[: int(len(full) * seg(t, 0.2, 1.0))]
    if t < 1.15:
        d.text(P(ax0 + 30, ay0 + ah - 54), typed, font=f(12), fill=WHITE)
    # кнопка отправки (пульс в момент нажатия)
    pulse = 1 + 0.25 * math.sin(math.pi * seg(t, 1.0, 1.3))
    r = 16 * pulse
    sx, sy = ax0 + aw - 38, ay0 + ah - 45
    d.ellipse(P(sx - r, sy - r, sx + r, sy + r), fill=BLUE)
    plane(d, sx, sy, 0, 9)

    # пузырь у отправителя
    checks = 0 if t < 2.6 else (1 if t < 4.0 else 2)
    bubble(d, ax0 + aw - 22, ay0 + 110, full, back(seg(t, 1.1, 1.45)), out=True, checks=checks)

    # облако-сервер
    glow = math.sin(math.pi * seg(t, 2.2, 3.0))
    cloud(d, *C, back(seg(t, 1.3, 1.8)), glow)

    # полёт самолётика: A -> облако -> B
    if 1.4 <= t <= 3.6:
        u = seg(t, 1.4, 3.6)
        if u < 0.5:
            k = ease(u / 0.5)
            p0, p1, p2 = (sx, sy), (420, 160), (C[0] - 10, C[1])
        else:
            k = ease((u - 0.5) / 0.5)
            p0, p1, p2 = (C[0] + 10, C[1]), (860, 160), (bx0 + 40, by0 + 125)
        # след
        for i in range(14):
            kk = clamp(k - i * 0.02)
            tx, ty = bezier(p0, p1, p2, kk)
            rr = 5 - i * 0.3
            d.ellipse(P(tx - rr, ty - rr, tx + rr, ty + rr), fill=(60 + 4 * (14 - i), 140, 210))
        x, y = bezier(p0, p1, p2, k)
        x2, y2 = bezier(p0, p1, p2, clamp(k + 0.01))
        plane(d, x, y, math.atan2(y2 - y, x2 - x), 22)

    # получатель: входящее сообщение + уведомление
    s = back(seg(t, 3.55, 3.9))
    bubble(d, bx0 + 22, by0 + 110, full, s, out=False)
    if t > 3.55:
        n = ease(seg(t, 3.55, 3.8)) * (1 - ease(seg(t, 4.6, 4.95)))
        if n > 0:
            ny = by0 - 40 + 30 * n
            d.rounded_rectangle(P(bx0 + 15, ny, bx0 + bw - 15, ny + 34), radius=12 * S, fill=GREEN)
            d.text(P(bx0 + 30, ny + 9), "Новое сообщение", font=f(13, True), fill=(10, 40, 25))

    return img.resize((W, H), Image.LANCZOS)


def main():
    with tempfile.TemporaryDirectory() as tmp:
        n = int(FPS * DUR)
        for i in range(n):
            frame(i / FPS).save(os.path.join(tmp, f"{i:04d}.png"))
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-framerate", str(FPS),
                        "-i", os.path.join(tmp, "%04d.png"), "-c:v", "libx264",
                        "-pix_fmt", "yuv420p", "-crf", "18", OUT], check=True)
    print(OUT)


if __name__ == "__main__":
    main()
