# slider.py

import asyncio
import random
import imagehash
from pyppeteer import launch

async def run():
    browser = await launch(headless=False)
    page = await browser.new_page()

    original_image = b''

    async def on_request(request):
        await request.continue_()

    async def on_response(response):
        if response.request.resourceType == 'image':
            nonlocal original_image
            original_image = await response.buffer()

    page.on('request', on_request)
    page.on('response', on_response)

    await page.goto('https://monoplasty.github.io/vue-monoplasty-slide-verify/')

    slider_element = await page.querySelector('.slide-verify-slider')
    slider = await slider_element.bounding_box()

    slider_handle = await page.querySelector('.slide-verify-slider-mask-item')
    handle = await slider_handle.bounding_box()

    current_position = 0
    best_slider = {
        'position': 0,
        'difference': 100
    }

    await page.mouse.move(handle['x'] + handle['width'] / 2, handle['y'] + handle['height'] / 2)
    await page.mouse.down()

    while current_position < slider['width'] - handle['width'] / 2:
        await page.mouse.move(
            handle['x'] + current_position,
            handle['y'] + handle['height'] / 2 + (random.random() * 10) - 5
        )

        slider_container = await page.querySelector('.slide-verify')
        slider_image = await slider_container.screenshot()

        hash1 = imagehash.average_hash(original_image)
        hash2 = imagehash.average_hash(slider_image)
        difference = hash1 - hash2

        if difference < best_slider['difference']:
            best_slider['difference'] = difference
            best_slider['position'] = current_position

        current_position += 5

    await page.mouse.move(
        handle['x'] + best_slider['position'],
        handle['y'] + handle['height'] / 2,
        {'steps': 10}
    )
    await page.mouse.up()

    await asyncio.sleep(3000)

    # success!

    await browser.close()

    await run()