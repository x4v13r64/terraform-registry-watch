import requests
import aiohttp
import zipfile
import io
import logging
import coloredlogs
import asyncio
import os

LOGGER = logging.getLogger('tf-registry-watch')


def get_all_pages(page_start=1, page_limit=100000):
    LOGGER.debug('Getting all pages')
    page = page_start
    responses = []
    next_page = True
    while next_page and page <= page_limit:
        if page == 1 or page % 10 == 0:
            LOGGER.debug(f'Fetching page {page}')
        url = f'https://registry.terraform.io/v2/providers?' \
              f'filter%5Btier%5D=official%2Cpartner%2Ccommunity&page%5Bnumber%5D={page}&page%5Bsize%5D=100'
        response = requests.get(url)
        response_dict = response.json()
        next_page = response_dict.get('links', {}).get('next') is not None
        page += 1
        responses.append(response_dict)
    LOGGER.debug(f'Returning {len(responses)} pages')
    return responses


def get_all_sources():
    LOGGER.debug('Getting all sources')
    sources = []
    for page in get_all_pages():
        for provider in page.get('data'):
            sources.append(provider.get('attributes').get('source'))
    LOGGER.debug(f'Returning {len(sources)} sources')
    return sources


def get_provider_details(provider_id):
    LOGGER.debug(f'Downloading details for provider {provider_id}')
    url = f'https://registry.terraform.io/v2/providers/{provider_id}?' \
          f'include=categories%2Cmoved-to%2Cpotential-fork-of%2Cprovider-versions%2Ctop-modules'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        LOGGER.error(f'Error getting provider {provider_id} details')


async def download_repository(session, repository,
                              branch='master', destination='/home/user/Downloads', overwrite=False):
    try:
        url = f'{repository}/archive/refs/heads/{branch}.zip'
        folder = f'{repository.split("/")[3]}-{repository.split("/")[4]}'
        tmp_location = f'{destination}/terraform-registry-watch/{folder}'
        if overwrite or not os.path.isdir(tmp_location):
            async with session.get(url) as response:
                if response.status == 200:
                    LOGGER.debug(f'Downloading repository {repository} to \"{tmp_location}\"')
                    content = await response.read()
                    z = zipfile.ZipFile(io.BytesIO(content))
                    z.extractall(tmp_location)
                # default branch is either master or main
                elif response.status == 404 and branch != 'main':
                    await download_repository(session, repository, branch='main')
                else:
                    LOGGER.warning(f'Error downloading repository {repository}: {response.status}')
        else:
            LOGGER.debug(f'Skipping repository {repository}, already exists')
    except Exception as e:
        LOGGER.error(f'Error downloading repository {repository}: {e}')


async def download_all_repositories():
    LOGGER.debug('Downloading all repositories')
    sources = get_all_sources()
    async with aiohttp.ClientSession() as session:
        tasks = []
        for source in sources:
            tasks.append(asyncio.ensure_future(
                download_repository(session, source)
            ))
        await asyncio.gather(*tasks)


# TODO support fetching all releases
# TODO support analysis via regexes
# TODO add 'download' and 'analyze' args to decouple both functionality
if __name__ == "__main__":
    coloredlogs.install(level='DEBUG', logger=LOGGER)

    LOGGER.info('Starting')

    asyncio.run(download_all_repositories())

    LOGGER.info('Done!')
