import os

DEBUG = os.environ.get('DEBUG', False) == 'True'

INTERNAL_IPS = [  # разрешённые ip доступа к django-debug-toolbar
    '127.0.0.1',
    '192.168.1.41',
]

if DEBUG:  # доп. из мануала, якобы для докера. хз, и так работает.
    import socket
    hostname, _, ips = socket.gethostbyname_ex(socket.gethostname())
    INTERNAL_IPS += [ip[: ip.rfind('.')] + '.1' for ip in ips] + ['10.0.2.2']
