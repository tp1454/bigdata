import { app } from './app'
import { config } from './config'

app.listen({ port: config.port, hostname: config.host })

console.log(`API listening on ${app.server?.hostname}:${app.server?.port}`)
