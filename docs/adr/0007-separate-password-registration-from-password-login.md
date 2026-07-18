# Separate password registration from password login

Administrators may disable self-service password registration without disabling password authentication. Turning registration off blocks only new public sign-ups: existing password credentials remain usable, administrators may still provision Hub Users, and enabled external Authentication Channels may still create or bind Hub Users. A separate policy is required to disable password login so changing registration policy cannot unexpectedly lock out existing users.
