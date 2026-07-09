import '@testing-library/jest-dom/vitest'
import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, cleanup, fireEvent } from '@testing-library/react'
import Button from './Button'
import StatusPill from './StatusPill'

afterEach(() => cleanup())

describe('Button', () => {
  it('renders its label and brand primary classes by default', () => {
    render(<Button>Save</Button>)
    const btn = screen.getByRole('button', { name: 'Save' })
    expect(btn.className).toContain('bg-primary')
    expect(btn).not.toBeDisabled()
  })

  it('is disabled and busy while loading', () => {
    render(<Button loading>Save</Button>)
    const btn = screen.getByRole('button', { name: 'Save' })
    expect(btn).toBeDisabled()
    expect(btn).toHaveAttribute('aria-busy', 'true')
    // spinner element present
    expect(btn.querySelector('.animate-spin')).not.toBeNull()
  })

  it('applies the danger variant', () => {
    render(<Button variant="danger">Delete</Button>)
    expect(screen.getByRole('button', { name: 'Delete' }).className).toContain('var(--danger)')
  })
})

describe('StatusPill', () => {
  it('renders nothing when idle', () => {
    const { container } = render(<StatusPill state="idle" />)
    expect(container.firstChild).toBeNull()
  })

  it('shows a spinner while saving', () => {
    const { container } = render(<StatusPill state="saving" />)
    expect(screen.getByText('Saving…')).toBeTruthy()
    expect(container.querySelector('.animate-spin')).not.toBeNull()
  })

  it('shows the saved label', () => {
    render(<StatusPill state="saved" />)
    expect(screen.getByText('Saved')).toBeTruthy()
  })

  it('fires onRetry from the error state', () => {
    const onRetry = vi.fn()
    render(<StatusPill state="error" onRetry={onRetry} />)
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    expect(onRetry).toHaveBeenCalledTimes(1)
  })
})
