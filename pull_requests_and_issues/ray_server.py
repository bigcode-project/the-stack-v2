import ray

try:
    from toolkit_run.ray.server import LabRayToolkitServer as LabRayServer

except Exception:
    print('Not running on toolkit, you need to implement your own ray cluster management')
    class LabRayServer():
        dashboard_url = None
        def scale_cluster(n):
            raise RuntimeError('LabRayServer.scale_cluster() is not implemented')

def get_ray_server():
    server = LabRayServer()
    print(server.dashboard_url)
    return server