"""
Mouse utilities for Custom Wheel Offset Scraper V2.
Provides human-like mouse movement and interaction functions.
"""

import random
import logging
import os
import importlib.util
from typing import Tuple, List
from playwright.async_api import Page

# Import local config from the parent directory
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
config_path = os.path.join(parent_dir, 'config.py')
spec = importlib.util.spec_from_file_location("local_config", config_path)
local_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local_config)

MOUSE_MOVE_STEPS = local_config.MOUSE_MOVE_STEPS
CLICK_SAFE_MARGIN = local_config.CLICK_SAFE_MARGIN
VIEWPORT = local_config.VIEWPORT

logger = logging.getLogger(__name__)


def generate_bezier_points(start: Tuple[int, int], end: Tuple[int, int], 
                          steps: int = MOUSE_MOVE_STEPS) -> List[Tuple[int, int]]:
    """
    Generate points along a Bezier curve for natural mouse movement.
    
    Args:
        start: Starting coordinates (x, y)
        end: Ending coordinates (x, y)
        steps: Number of steps in the movement
        
    Returns:
        List of coordinate tuples for the movement path
    """
    x1, y1 = start
    x4, y4 = end
    
    # Generate control points for natural curve
    x2 = x1 + random.randint(-100, 100)
    y2 = y1 + random.randint(-100, 100)
    x3 = x4 + random.randint(-100, 100)
    y3 = y4 + random.randint(-100, 100)
    
    points = []
    for i in range(steps + 1):
        t = i / steps
        
        # Cubic Bezier curve calculation
        x = int((1-t)**3 * x1 + 3*(1-t)**2 * t * x2 + 
                3*(1-t) * t**2 * x3 + t**3 * x4)
        y = int((1-t)**3 * y1 + 3*(1-t)**2 * t * y2 + 
                3*(1-t) * t**2 * y3 + t**3 * y4)
        
        points.append((x, y))
    
    return points


def get_safe_click_area() -> Tuple[int, int]:
    """
    Get a safe area within the viewport for clicking.
    
    Returns:
        Random coordinates within safe clicking area
    """
    margin = CLICK_SAFE_MARGIN
    width = VIEWPORT["width"]
    height = VIEWPORT["height"]
    
    x = random.randint(margin, width - margin)
    y = random.randint(margin, height - margin)
    
    return x, y


async def move_mouse_naturally(page: Page, target_x: int, target_y: int) -> None:
    """
    Move mouse to target coordinates using natural Bezier curve movement.
    
    Args:
        page: Playwright page instance
        target_x: Target X coordinate
        target_y: Target Y coordinate
    """
    # Get current mouse position (approximate center as starting point)
    start_x = VIEWPORT["width"] // 2
    start_y = VIEWPORT["height"] // 2
    
    # Generate natural movement path
    points = generate_bezier_points((start_x, start_y), (target_x, target_y))
    
    # Move along the path
    for x, y in points:
        await page.mouse.move(x, y)
        # Small delay between movements
        await page.wait_for_timeout(random.randint(1, 5))
    
    logger.debug(f"Mouse moved naturally to ({target_x}, {target_y})")


async def click_safe_area(page: Page) -> None:
    """
    Click in a safe area of the page to simulate human activity.
    
    Args:
        page: Playwright page instance
    """
    x, y = get_safe_click_area()
    await move_mouse_naturally(page, x, y)
    await page.mouse.click(x, y)
    logger.debug(f"Clicked safe area at ({x}, {y})")