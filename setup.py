from setuptools import setup

setup(
        name='datasift_preview_grabber',
        version='0.0.0',
        description='Grabs Datasift preview stats and writes them out to stdout',
        author='Adaptive Lab',
        author_email='hey@adaptivelab.com',
        url='https://github.com/adaptivelab/datasift-preview-grabber',
        download_url='https://github.com/adaptivelab/datasift-preview-grabber',
        scripts=['datasift_preview_grabber.py'],
        entry_points = {
            'console_scripts': [
                'datasift_preview_grabber = datasift_preview_grabber:main'
            ]
        },
        license='GPLv3',
        install_requires=[
            "datasift",
            "python-dateutil",
            "docopt",
            "pytz"
        ],
)