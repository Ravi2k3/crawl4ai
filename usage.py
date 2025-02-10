import undetected_chromedriver as uc, random, time, os, html2text, logging, asyncio
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse
from xvfbwrapper import Xvfb
from src.rag_system import RAGSystem
from textwrap import dedent
from pyppeteer import launch

logger = logging.getLogger("hive")

class AsyncContentExtractor:
    def __init__(self, max_concurrent=10):
        self.max_concurrent = max_concurrent
        self.semaphore = None
        self.browser = None
        self.connection_retries = 3
        
    async def init(self):
        """Initialize browser instance with retries"""
        if not self.browser:
            for attempt in range(self.connection_retries):
                try:
                    self.browser = await launch({
                        'args': [
                            '--no-sandbox',
                            '--disable-setuid-sandbox',
                            '--disable-dev-shm-usage',
                            '--disable-accelerated-2d-canvas',
                            '--disable-gpu',
                            '--window-size=1920,1080',
                            '--disable-web-security',
                            '--disable-features=IsolateOrigins,site-per-process',
                            '--disable-blink-features=AutomationControlled',
                            '--disable-infobars',
                            '--disable-popup-blocking',
                            '--disable-notifications',
                            '--disable-logging',
                            '--disable-extensions',
                            '--disable-default-apps',
                            '--disable-component-extensions-with-background-pages',
                            '--disable-client-side-phishing-detection',
                            '--disable-component-update',
                            '--disable-background-networking',
                            '--disable-sync',
                            '--metrics-recording-only',
                            '--mute-audio',
                            '--no-first-run',
                            '--no-default-browser-check',
                            '--autoplay-policy=no-user-gesture-required',
                            '--disable-background-timer-throttling',
                            '--disable-renderer-backgrounding',
                            '--disable-backgrounding-occluded-windows',
                            '--disable-ipc-flooding-protection',
                            '--disable-hang-monitor',
                            '--disable-breakpad',
                            '--disable-crash-reporter',
                            '--disable-device-discovery-notifications',
                            '--disable-translate',
                            '--disable-software-rasterizer',
                            '--disable-remote-fonts',
                        ],
                        'ignoreHTTPSErrors': True
                    })
                    break
                except Exception as e:
                    logger.error(f"Browser launch attempt {attempt + 1} failed: {str(e)}")
                    if attempt == self.connection_retries - 1:
                        raise
                    await asyncio.sleep(1)
                    
        if not self.semaphore:
            self.semaphore = asyncio.Semaphore(self.max_concurrent)

    async def close(self):
        """Clean up resources safely"""
        if self.browser:
            try:
                await asyncio.wait_for(self.browser.close(), timeout=5.0)
            except:
                pass
            self.browser = None

    async def extract_content(self, url):
        """Extract clean content from a single URL"""
        async with self.semaphore:
            page = None
            try:
                page = await self.browser.newPage()
                
                # Configure stealth settings
                await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
                await page.setExtraHTTPHeaders({
                    'Accept-Language': 'en-US,en;q=0.9'
                })
                await page.evaluateOnNewDocument('''() => {
                    delete navigator.__proto__.webdriver;
                }''')

                await page.setViewport({'width': 1920, 'height': 1080})
                await page.setRequestInterception(True)
                
                async def intercept(request):
                    if request.resourceType in ['image', 'stylesheet', 'font', 'media', 'script']:
                        await request.abort()
                    else:
                        await request.continue_()

                page.on('request', lambda req: asyncio.ensure_future(intercept(req)))
                
                # Navigation with retries
                content = None
                for attempt in range(3):
                    try:
                        response = await page.goto(url, {
                            'waitUntil': 'networkidle2',
                            'timeout': 15000
                        })
                        
                        if response.status == 403:
                            raise Exception("Cloudflare block detected")
                            
                        await asyncio.sleep(random.uniform(0.5, 1.5))

                        content = await page.evaluate('''() => {
                            const selectors = [
                                'article', 'main', '[role="main"]', 
                                '.post-content', '.article-content',
                                '.entry-content', '#content-main',
                                '.content', '.text', '.body'
                            ];

                            let mainContent = document.body;
                            for (const selector of selectors) {
                                const el = document.querySelector(selector);
                                if (el && el.textContent.trim().length > 500) {
                                    mainContent = el;
                                    break;
                                }
                            }

                            // Remove unwanted elements
                            const unwanted = [
                                'script', 'style', 'img', 'svg', 'button', 
                                'form', 'iframe', 'noscript', 'nav', 'footer',
                                'header', 'aside', 'input', 'textarea', 'select',
                                'object', 'embed', 'video', 'audio', 'source',
                                'track', 'canvas', 'map', 'area', 'figure',
                                'picture', 'link', 'meta', '[role*="navigation"]',
                                '[class*="ad"]', '[id*="ad"]', '[class*="banner"]',
                                '[class*="cookie"]', '[class*="modal"]', '.hidden',
                                '[aria-hidden="true"]'
                            ];

                            unwanted.forEach(selector => {
                                mainContent.querySelectorAll(selector).forEach(el => el.remove());
                            });

                            // Clean inline attributes and links
                            mainContent.querySelectorAll('*').forEach(el => {
                                el.removeAttribute('style');
                                el.removeAttribute('onclick');
                                el.removeAttribute('href');
                            });

                            // Convert links to plain text
                            mainContent.querySelectorAll('a').forEach(a => {
                                const txt = document.createTextNode(a.textContent);
                                a.parentNode.replaceChild(txt, a);
                            });

                            return mainContent.textContent
                                .replace(/\\s+/g, ' ')
                                .replace(/\\[.*?\\]/g, '')
                                .trim();
                        }''')

                        if content and len(content) > 500:
                            break

                    except Exception as e:
                        logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                        await asyncio.sleep(1)

                return {
                    'url': url,
                    'content': content if content and len(content) > 500 else None,
                    'error': None
                }
                
            except Exception as e:
                return {'url': url, 'content': None, 'error': str(e)}
            finally:
                if page:
                    try:
                        await page.close()
                    except:
                        pass

    async def process_batch(self, urls):
        """Process URLs in parallel with optimized resource management"""
        await self.init()
        results = []
        
        for i in range(0, len(urls), 10):
            chunk = urls[i:i + 10]
            tasks = [self.extract_content(url) for url in chunk]
            
            try:
                chunk_results = await asyncio.gather(*tasks, return_exceptions=True)
                valid_results = [r for r in chunk_results if not isinstance(r, Exception)]
                results.extend(valid_results)
                await asyncio.sleep(random.uniform(0.5, 1.5))
            except Exception as e:
                logger.error(f"Error processing chunk: {str(e)}")
                
        return results

class GoogleSearcher:
    def __init__(self, headless=True, banned_links=None):
        self._setup_html2text()
        self.driver = None
        self.headless = headless
        self.xvfb = None
        self.banned_links = set(banned_links or [])
        
    def _setup_html2text(self):
        self.h2t = html2text.HTML2Text()
        self.h2t.ignore_links = True
        self.h2t.ignore_images = True
        self.h2t.ignore_tables = True
        self.h2t.body_width = 0

    def _get_chrome_options(self):
        options = uc.ChromeOptions()
        viewport = random.choice([
            (1366, 768), (1920, 1080), 
            (1536, 864), (1440, 900)
        ])
        
        if os.path.exists("/usr/bin/google-chrome"):
            options.binary_location = "/usr/bin/google-chrome"
        
        options.add_argument(f'--window-size={viewport[0]},{viewport[1]}')
        options.add_argument('--disable-blink-features=AutomationControlled')
        
        if self.headless:
            self.xvfb = Xvfb(width=viewport[0], height=viewport[1])
            self.xvfb.start()

        return options

    def _natural_scroll(self, driver):
        driver.execute_script("""
            window.scrollTo({
                top: document.body.scrollHeight,
                behavior: 'smooth'
            })
        """)
        time.sleep(random.uniform(0.5, 1.2))

    def search(self, query: str, num_results: int = 10):
        """Main search method with integrated content extraction"""
        try:
            logger.info(f"Searching for: {query}")
            self.driver = uc.Chrome(
                options=self._get_chrome_options(),
                use_subprocess=True
            )
            
            results = []
            seen_urls = set()
            
            for page_num in range(1, 4):
                logger.info(f"Processing page {page_num}")
                if len(results) >= num_results:
                    break
                    
                try:
                    if page_num == 1:
                        self.driver.get("https://www.google.com")
                        search_box = WebDriverWait(self.driver, 3).until(
                            EC.presence_of_element_located((By.NAME, "q"))
                        )
                        search_box.send_keys(query + Keys.RETURN)
                    else:
                        # Wait for results to load before looking for next button
                        WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.g"))
                        )
                        
                        # Check if next button exists
                        try:
                            next_button = WebDriverWait(self.driver, 3).until(
                                EC.element_to_be_clickable((By.ID, "pnnext"))
                            )
                            next_button.click()
                        except:
                            logger.info(f"No more pages available after page {page_num}")
                            break
                        
                    # Wait for results to load
                    WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.g"))
                    )
                    
                    self._natural_scroll(self.driver)
                    time.sleep(random.uniform(1, 2))
                    
                    search_results = self.driver.find_elements(By.CSS_SELECTOR, "div.g")
                    if not search_results:
                        logger.warning(f"No results found on page {page_num}")
                        break
                        
                    for result in search_results:
                        try:
                            url = next((
                                link.get_attribute("href") 
                                for link in result.find_elements(By.TAG_NAME, "a")
                                if link.get_attribute("href") and "google.com" not in link.get_attribute("href")
                            ), None)
                            
                            if not url or url in seen_urls or url in self.banned_links:
                                continue
                                
                            title = next((
                                el.text.strip() 
                                for el in result.find_elements(By.CSS_SELECTOR, "h3, h2, h1")
                                if el.text.strip()
                            ), urlparse(url).netloc.split('.')[0].capitalize())
                            
                            seen_urls.add(url)
                            results.append({'url': url, 'title': title})
                            
                        except Exception as e:
                            continue
                            
                except Exception as e:
                    logger.error(f"Page {page_num} error: {str(e)}")
                    break
            
            logger.info(f"Extracting content from {len(results)} URLs")
            # Process content extraction
            loop = asyncio.new_event_loop()
            extractor = AsyncContentExtractor()
            content_results = loop.run_until_complete(
                extractor.process_batch([r['url'] for r in results[:num_results]])
            )
            loop.run_until_complete(extractor.close())
            loop.close()
            
            # Merge results
            final_results = []
            for res in content_results:
                if res['content']:
                    match = next(r for r in results if r['url'] == res['url'])
                    final_results.append({
                        'title': match['title'],
                        'url': res['url'],
                        'content': res['content']
                    })
            
            logger.info(f"Found {len(final_results)} valid results")
            return final_results[:num_results]
            
        finally:
            if self.driver:
                self.driver.quit()
            if self.xvfb:
                self.xvfb.stop()
    
class WebSearch:
    def __init__(self, query: str):
        self.query = query
        self.scraper = GoogleSearcher()
        self.results = []
        self.output = []
        self.search()
    
    def search(self) -> None:
        self.results = self.scraper.search(self.query, num_results=3)
        self.rag_system = RAGSystem(namespace="web_rag", max_sources=2)

        for result in self.results:
            text = f"Title: {result['title']}\nURL: {result['url']}\nContent: {result['content']}"
            self.rag_system.add_text(text)
            rag_output = self.rag_system.query(self.query)
            self.rag_system.clear_rag()
            result['content'] = result['content'][:1000] + "..."
            self.output.append(
                dedent(f"""
                Title: 
                ```
                {result['title']}
                ```
                
                URL: 
                ```
                {result['url']}
                ```
                
                Content: 
                ```
                {result['content']}
                ```
                
                Localized Content:  
                ```
                {rag_output}
                ```
                """).strip()
            )
