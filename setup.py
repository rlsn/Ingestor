from setuptools import setup, find_packages

setup(
    name='pygestor',
    version='0.2.0',
    url='https://github.com/rlsn/Pygestor',
    author='Yumo Wang',
    author_email='yumo1996@gmail.com',
    description='A data platform designed to efficiently acquire, organize, and manage diverse datasets, ensuring seamless access and usability for AI researchers.',
    packages=find_packages(),    
    install_requires=[
        'pyarrow >= 17.0.0',
        'huggingface-hub>=0.24.5',
        'pillow>=10.4.0',
        'nicegui>=1.4.34',
        'pyperclip>=1.9.0'
        ],
)