# Automation Runtime

TypeScript workflows executed on an explicitly selected device.

This optional plugin is loaded by the Taskasaur plugin framework. Its code and collection schemas are released independently of the client/server. Required core providers are supplied by Taskasaur.

```sh
npm ci
npm run build
```

Package `dist/` with the server's `npm run plugins -- pack` command and a private publisher key. Upload the signed ZIP to any HTTPS host and register its immutable download URL and SHA-256 in [taskasaur-plugin-inventory](https://github.com/taskasaur/taskasaur-plugin-inventory). See that repository's `docs/plugin-specification.md` for the complete interface and release workflow. No private signing key belongs in this repository.
