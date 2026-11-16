import { Elysia } from 'elysia'
import { cors } from '@elysiajs/cors'

import { config } from './config'
import { crypto } from './modules/crypto'
import { equities } from './modules/equities'
import { forex } from './modules/forex'
import { health } from './modules/health'
import { errors } from './plugins/errors'
import { openapiPlugin } from './plugins/openapi'

const app = new Elysia()
  .use(cors())
  .use(openapiPlugin)
  .use(errors)
  .use(health)
  .use(crypto)
  .use(equities)
  .use(forex)
  .listen({ port: config.port, hostname: config.host })

console.log(`API listening on ${app.server?.hostname}:${app.server?.port}`)
