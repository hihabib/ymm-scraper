import asyncio
import random
from playwright.async_api import Page


async def human_type(page: Page, element, text: str, typing_speed: str = "normal") -> None:
    """Type text with human-like patterns including variable delays, occasional typos, and corrections."""
    speed_configs = {
        "slow": {"base_delay": 0.15, "variance": 0.1, "typo_chance": 0.02},
        "normal": {"base_delay": 0.08, "variance": 0.06, "typo_chance": 0.03},
        "fast": {"base_delay": 0.04, "variance": 0.04, "typo_chance": 0.05},
    }
    config = speed_configs.get(typing_speed, speed_configs["normal"])

    await element.click()
    await asyncio.sleep(random.uniform(0.1, 0.3))
    await element.fill("")

    i = 0
    while i < len(text):
        char = text[i]

        if random.random() < config["typo_chance"] and char.isalpha():
            wrong_chars = "qwertyuiopasdfghjklzxcvbnm"
            wrong_char = random.choice(wrong_chars)
            await element.type(wrong_char)
            await asyncio.sleep(random.uniform(0.2, 0.5))
            await page.keyboard.press("Backspace")
            await asyncio.sleep(random.uniform(0.1, 0.3))

        await element.type(char)

        if char == " ":
            delay = random.uniform(config["base_delay"] * 2, config["base_delay"] * 4)
        elif char in ".,!?;:":
            delay = random.uniform(config["base_delay"] * 1.5, config["base_delay"] * 3)
        elif i > 0 and text[i - 1] == char:
            delay = random.uniform(config["base_delay"] * 0.5, config["base_delay"])
        else:
            delay = random.uniform(
                config["base_delay"] - config["variance"],
                config["base_delay"] + config["variance"],
            )

        await asyncio.sleep(max(0.02, delay))
        i += 1

    await asyncio.sleep(random.uniform(0.3, 0.8))


async def human_delay(min_seconds: float = 1.0, max_seconds: float = 3.0) -> float:
    """Add human-like random delays and return the delay used."""
    base_delay = random.uniform(min_seconds, max_seconds)
    network_delay = random.uniform(0.05, 0.3)
    if random.random() < 0.1:
        base_delay += random.uniform(2, 8)
    total_delay = base_delay + network_delay
    await asyncio.sleep(total_delay)
    return total_delay


async def network_delay(request_type: str = "normal") -> None:
    """Add realistic network delays based on request type."""
    delay_configs = {
        "dns": random.uniform(0.02, 0.1),
        "connect": random.uniform(0.05, 0.2),
        "ssl": random.uniform(0.1, 0.3),
        "request": random.uniform(0.02, 0.08),
        "response": random.uniform(0.1, 0.5),
        "normal": random.uniform(0.05, 0.2),
    }
    delay = delay_configs.get(request_type, delay_configs["normal"])
    await asyncio.sleep(delay)


def _generate_bezier_control_points(start_x: float, start_y: float, end_x: float, end_y: float):
    distance = ((end_x - start_x) ** 2 + (end_y - start_y) ** 2) ** 0.5
    mid_x = (start_x + end_x) / 2
    mid_y = (start_y + end_y) / 2
    offset_distance = distance * random.uniform(0.1, 0.3)
    angle_offset = random.uniform(-1, 1)
    control1_x = start_x + (mid_x - start_x) * 0.3 + offset_distance * angle_offset
    control1_y = start_y + (mid_y - start_y) * 0.3 + offset_distance * (1 - abs(angle_offset))
    control2_x = end_x - (end_x - mid_x) * 0.3 + offset_distance * angle_offset * 0.5
    control2_y = end_y - (end_y - mid_y) * 0.3 + offset_distance * (1 - abs(angle_offset)) * 0.5
    return [(start_x, start_y), (control1_x, control1_y), (control2_x, control2_y), (end_x, end_y)]


def _calculate_bezier_point(control_points, t: float):
    p0, p1, p2, p3 = control_points
    x = (1 - t) ** 3 * p0[0] + 3 * (1 - t) ** 2 * t * p1[0] + 3 * (1 - t) * t ** 2 * p2[0] + t ** 3 * p3[0]
    y = (1 - t) ** 3 * p0[1] + 3 * (1 - t) ** 2 * t * p1[1] + 3 * (1 - t) * t ** 2 * p2[1] + t ** 3 * p3[1]
    return x, y


async def human_mouse_movement(page: Page) -> None:
    """Simulate human-like mouse movements with natural patterns."""
    viewport = page.viewport_size or {"width": 1280, "height": 720}
    num_movements = random.randint(3, 7)
    current_x, current_y = viewport["width"] // 2, viewport["height"] // 2

    for _ in range(num_movements):
        target_x = random.randint(50, viewport["width"] - 50)
        target_y = random.randint(50, viewport["height"] - 50)
        control_points = _generate_bezier_control_points(current_x, current_y, target_x, target_y)
        steps = random.randint(15, 30)
        for step in range(steps):
            t = step / (steps - 1)
            x, y = _calculate_bezier_point(control_points, t)
            jitter_x = random.uniform(-2, 2)
            jitter_y = random.uniform(-2, 2)
            final_x = max(0, min(viewport["width"], x + jitter_x))
            final_y = max(0, min(viewport["height"], y + jitter_y))
            await page.mouse.move(final_x, final_y)
            if step < 3 or step > steps - 4:
                delay = random.uniform(0.02, 0.05)
            else:
                delay = random.uniform(0.01, 0.03)
            await asyncio.sleep(delay)
        current_x, current_y = target_x, target_y
        await asyncio.sleep(random.uniform(0.1, 0.4))
        if random.random() < 0.3:
            for _ in range(random.randint(2, 4)):
                micro_x = current_x + random.uniform(-5, 5)
                micro_y = current_y + random.uniform(-5, 5)
                await page.mouse.move(micro_x, micro_y)
                await asyncio.sleep(random.uniform(0.05, 0.1))


async def human_scroll(page: Page) -> None:
    """Simulate human-like scrolling behavior."""
    for _ in range(random.randint(2, 4)):
        scroll_amount = random.randint(200, 800)
        await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
        await asyncio.sleep(random.uniform(0.5, 1.5))
    if random.random() < 0.3:
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(random.uniform(0.5, 1.0))