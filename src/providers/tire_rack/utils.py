"""
Utility functions for Tire Rack scraper.

Currently provides:
- extract_option_values(html: str, include_placeholder: bool = False) -> list[str]
  Parses an HTML fragment and returns all <option> value attributes as a list of strings.
"""

from typing import List, Dict
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from core.errors import ParsingError


def extract_option_values(html: str, include_placeholder: bool = False) -> List[str]:
    """
    Extract <option> value attributes from the <select id="vehicle-make"> element
    within the given HTML string.

    - html: HTML fragment or document containing the target <select> element.
    - include_placeholder: when False (default), commonly used placeholder values
      like "#" and empty strings are filtered out.

    Returns a list of strings.
    """
    soup = BeautifulSoup(html, "html.parser")

    select = soup.find("select", id="vehicle-make")
    if not select:
        raise ParsingError("Expected <select id='vehicle-make'> not found")

    values: List[str] = []
    for opt in select.find_all("option"):
        val = opt.get("value")
        if val is None:
            continue
        val = val.strip()
        if not include_placeholder and (val == "" or val == "#"):
            continue
        values.append(val)
    if not values:
        raise ParsingError("No <option> values extracted for makes")
    return values


__all__ = ["extract_option_values"]
def extract_xml_values(xml_str: str, tag_name: str) -> List[str]:
    """
    Extract text values for all elements with the given tag name from the XML string.

    Example: xml_str = "<years><year>2026</year><year>2025</year></years>", tag_name = "year"
    Returns: ["2026", "2025"]
    """
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError as e:
        raise ParsingError(f"XML parsing failed: {e}")

    values: List[str] = []
    for elem in root.iter(tag_name):
        text = (elem.text or "").strip()
        if text:
            values.append(text)
    return values

__all__ = ["extract_option_values", "extract_xml_values"]

def extract_tire_sizes(html: str) -> Dict[str, List[Dict[str, str]]]:
    f"""
    Parse tire sizes from the HTML string.

    Targets two lists:
    - <ul id="oeSizes"> ... </ul>
    - <ul id="optionalSizes"> ... </ul>

    For each <li class="optionWrapBtn"> inside those lists, extract:
    - width: text from <span class="sizeWidth">
    - tire_size: text from <span class="sizeDetail">
    - size_msg: text from <span class="sizeMsg"> if present, otherwise ""

    Returns a dictionary: {"oe_sizes": [{...},...], "optional_sizes": [{...},...]}.
    Hidden inputs or non-<li> elements are ignored.
    """
    soup = BeautifulSoup(html, "html.parser")

    def _extract_from_ul(ul_id: str) -> List[Dict[str, str]]:
        ul = soup.find("ul", id=ul_id)
        results: List[Dict[str, str]] = []
        if not ul:
            return results

        for li in ul.find_all("li", class_="optionWrapBtn"):
            size_width_el = li.find("span", class_="sizeWidth")
            size_detail_el = li.find("span", class_="sizeDetail")
            size_msg_el = li.find("span", class_="sizeMsg")

            width = size_width_el.get_text(strip=True) if size_width_el else ""
            tire_size = size_detail_el.get_text(strip=True) if size_detail_el else ""
            size_msg = size_msg_el.get_text(strip=True) if size_msg_el else ""

            # Only include entries that have a tire size
            if tire_size:
                results.append({
                    "width": width,
                    "tire_size": tire_size,
                    "size_msg": size_msg,
                })

        return results

    return {
        "oe_sizes": _extract_from_ul("oeSizes"),
        "optional_sizes": _extract_from_ul("optionalSizes"),
    }

__all__ = ["extract_option_values", "extract_xml_values", "extract_tire_sizes"]