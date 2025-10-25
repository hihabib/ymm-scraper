function extractFitmentData() {
    const isMerged = document.querySelector(".store-ymm-fitrange.full-size") !== null;

    const parseRange = (text) => {
        if (!text || !text.includes("to")) return { min: null, max: null };
        const match = text.trim().match(/^(.+?)\s+to\s+(.+)$/);
        if (!match) return { min: null, max: null };
        const min = match[1].trim();
        const max = match[2].trim();
        if (!min || !max || min === '"' || max === '"' || min === 'mm' || max === 'mm') {
            return { min: null, max: null };
        }
        return { min, max };
    };

    const parseBoltPattern = () => {
        const raw = document.querySelector(".store-bp")?.getAttribute("data-bp")?.trim() || "";
        const [inchRaw, mmRaw] = raw.split(",").map(s => s?.trim());
        return {
            inch: inchRaw ? inchRaw + '"' : null,
            mm: mmRaw ? mmRaw + "mm" : null
        };
    };

    const extractFromElement = (element) => {
        const getText = (label) => {
            const span = Array.from(element.querySelectorAll(".store-conf-range"))
                .find(el => el.textContent.trim().startsWith(label));
            return span?.querySelector("b")?.textContent.trim() || "";
        };

        return {
            diameter: parseRange(getText("Diameter:")),
            width: parseRange(getText("Width:")),
            offset: parseRange(getText("Offset:"))
        };
    };

    if (isMerged) {
        const mergedEl = document.querySelector(".store-ymm-fitrange.full-size");
        const shared = extractFromElement(mergedEl);
        return { front: shared, rear: shared };
    }

    const getSectionElement = (headerText) =>
        Array.from(document.querySelectorAll(".store-ymm-fitrange"))
            .find(el => el.querySelector("nobr")?.textContent.includes(headerText));

    const defaultValues = {
        diameter: { min: null, max: null },
        width: { min: null, max: null },
        offset: { min: null, max: null }
    };

    const frontEl = getSectionElement("Front");
    const rearEl = getSectionElement("Rear");

    const front = frontEl ? extractFromElement(frontEl) : defaultValues;
    const rear = rearEl ? extractFromElement(rearEl) : defaultValues;

    return { front, rear };
}

console.log(JSON.stringify(extractFitmentData()))