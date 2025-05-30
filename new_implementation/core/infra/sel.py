"""
Playwright helpers for browser automation and shadow DOM interaction.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

try:
    from playwright.async_api import async_playwright, Browser, Page, BrowserContext
except ImportError:
    async_playwright = None
    Browser = None
    Page = None
    BrowserContext = None


logger = logging.getLogger(__name__)


class PlaywrightClient:
    """Async Playwright client for browser automation."""
    
    def __init__(
        self,
        headless: bool = True,
        browser_type: str = "chromium",
        timeout: float = 30000,
    ):
        if async_playwright is None:
            raise ImportError("playwright package is required for browser automation")
        
        self.headless = headless
        self.browser_type = browser_type
        self.timeout = timeout
        
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    async def start(self) -> None:
        """Start the browser."""
        if self._browser:
            return
        
        self._playwright = await async_playwright().start()
        
        if self.browser_type == "chromium":
            browser_launcher = self._playwright.chromium
        elif self.browser_type == "firefox":
            browser_launcher = self._playwright.firefox
        elif self.browser_type == "webkit":
            browser_launcher = self._playwright.webkit
        else:
            raise ValueError(f"Unsupported browser type: {self.browser_type}")
        
        self._browser = await browser_launcher.launch(headless=self.headless)
        self._context = await self._browser.new_context()
        
        logger.info(f"Started {self.browser_type} browser (headless={self.headless})")

    async def stop(self) -> None:
        """Stop the browser."""
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        
        logger.info("Stopped browser")

    async def new_page(self) -> Page:
        """Create a new page."""
        if not self._context:
            await self.start()
        
        page = await self._context.new_page()
        page.set_default_timeout(self.timeout)
        return page

    async def get_page_content(self, url: str, wait_for_selector: Optional[str] = None) -> str:
        """Get the HTML content of a page."""
        page = await self.new_page()
        try:
            await page.goto(url)
            
            if wait_for_selector:
                await page.wait_for_selector(wait_for_selector)
            
            return await page.content()
        finally:
            await page.close()

    async def extract_shadow_dom_content(
        self,
        url: str,
        shadow_host_selector: str,
        content_selector: str,
    ) -> Optional[str]:
        """Extract content from within a shadow DOM."""
        page = await self.new_page()
        try:
            await page.goto(url)
            
            # Wait for shadow host to be available
            await page.wait_for_selector(shadow_host_selector)
            
            # Access shadow root and extract content
            shadow_content = await page.evaluate(f"""
                () => {{
                    const host = document.querySelector('{shadow_host_selector}');
                    if (!host || !host.shadowRoot) return null;
                    
                    const element = host.shadowRoot.querySelector('{content_selector}');
                    return element ? element.textContent : null;
                }}
            """)
            
            return shadow_content
        finally:
            await page.close()

    async def fill_form_and_submit(
        self,
        url: str,
        form_data: Dict[str, str],
        submit_selector: str,
        wait_for_navigation: bool = True,
    ) -> str:
        """Fill out a form and submit it."""
        page = await self.new_page()
        try:
            await page.goto(url)
            
            # Fill form fields
            for selector, value in form_data.items():
                await page.fill(selector, value)
            
            # Submit form
            if wait_for_navigation:
                async with page.expect_navigation():
                    await page.click(submit_selector)
            else:
                await page.click(submit_selector)
            
            return await page.content()
        finally:
            await page.close()

    async def wait_for_element_and_extract(
        self,
        url: str,
        selector: str,
        attribute: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Optional[str]:
        """Wait for an element to appear and extract its content or attribute."""
        page = await self.new_page()
        try:
            await page.goto(url)
            
            element = await page.wait_for_selector(selector, timeout=timeout or self.timeout)
            
            if attribute:
                return await element.get_attribute(attribute)
            else:
                return await element.text_content()
        finally:
            await page.close()
