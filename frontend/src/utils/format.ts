export function formatBytes(n: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  let value = n
  let unit = 0
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024
    unit++
  }
  return `${unit === 0 ? value : value.toFixed(1)} ${units[unit]}`
}

export function truncateId(id: string, chars = 10): string {
  if (id.length <= chars * 2) return id
  return `${id.slice(0, chars)}…${id.slice(-4)}`
}

export async function copyToClipboard(text: string): Promise<void> {
  await navigator.clipboard.writeText(text)
}
