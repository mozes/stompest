from setuptools import setup, find_packages
setup(
    name = "stompest",
    version = "0.1",
    author = "Roger Hoover",
    author_email = "roger.hoover@gmail.com",
    description = "No nonsense Python STOMP client",
    license = 'Apache License 2.0',
    packages = find_packages(),
    py_modules=["stompest"],
    include_package_data = True,
    zip_safe = False,   
    install_requires = [
        'stomper',
        'twisted',
        'mock',
    ],
    test_suite = 'stompest.tests',
)

