# Allow unrestricted personal model endpoints

Personal Model Connections may target any user-supplied HTTP or HTTPS address, including loopback, private-network, link-local, and cloud metadata destinations. This deliberately prioritizes unrestricted access to self-hosted and internal Responses providers over SSRF isolation: ordinary users can cause Hub to send requests into networks reachable by Hub, and this risk is accepted rather than mitigated by URL allowlists, IP or DNS filtering, or redirect restrictions.
