declare module '@rjsf/validator-ajv8' {
  import type { ValidatorType, RJSFSchema } from '@rjsf/utils'
  const validator: ValidatorType<unknown, RJSFSchema, unknown>
  export default validator
}
