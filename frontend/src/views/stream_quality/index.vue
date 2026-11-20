<template>
  <section class="page" data-module="stream_quality">
    <header class="page-head">
      <div>
        <h2>流式质量管理</h2>
        <p class="page-desc">维护流式数据实时完整性、延迟与分布的质量监控规则，为流式质量提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记质量规则</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出流式质量清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <div class="batch-bar">
      <button
        class="btn primary"
        data-testid="run-batch"
        type="button"
        :disabled="!selectedIds.length || batchRunning"
        @click="runBatch"
      >
        {{ batchRunning ? '巡检中…' : `批量巡检选中规则（${selectedIds.length}）` }}
      </button>
      <label class="batch-toggle">
        <input v-model="simulateInterrupt" type="checkbox" data-testid="simulate-interrupt" />
        模拟写入中断（验证断点续做）
      </label>
      <span class="batch-hint">多选整组处理，巡检结果写入下方巡检批次表</span>
    </div>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
          <th>
            <input
              type="checkbox"
              data-testid="select-all"
              :checked="allSelected"
              title="全选当前页"
              @change="toggleSelectAll"
            />
          </th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
          <td class="row-select">
            <input
              type="checkbox"
              :data-testid="`select-${row.id}`"
              :checked="selectedIds.includes(Number(row.id))"
              @change="toggleSelect(Number(row.id))"
            />
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 2" class="empty-state">暂无流式质量数据，可先登记质量规则</td>
        </tr>
      </tbody>
    </table>

    <section class="batch-section" data-testid="batch-section">
      <header class="batch-head">
        <h3>巡检批次表</h3>
        <span class="batch-hint">按检测窗口去重，保留窗口去重结果与续做边界；中断批次可从断点恢复</span>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>批次编号</th>
            <th>主题列表</th>
            <th>来源版本</th>
            <th>去重口径</th>
            <th>窗口总数</th>
            <th>已检查</th>
            <th>已去重</th>
            <th>续做边界</th>
            <th>状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <template v-for="batch in batches" :key="String(batch.id)">
            <tr :data-testid="`batch-${batch.id}`" :data-status="String(batch.status)">
              <td>{{ batch['批次编号'] }}</td>
              <td>{{ (batch['主题列表'] || []).join('、') }}</td>
              <td>{{ batch['来源版本'] }}</td>
              <td>{{ batch['去重口径'] }}</td>
              <td>{{ batch['窗口总数'] }}</td>
              <td>{{ batch['已检查数'] }}</td>
              <td>{{ batch['已去重数'] }}</td>
              <td>{{ batch['续做边界'] }}/{{ batch['窗口总数'] }}</td>
              <td>{{ batch.status }}</td>
              <td class="row-actions">
                <button
                  v-if="batch.status === '已中断'"
                  class="link"
                  type="button"
                  :data-testid="`resume-${batch.id}`"
                  @click="resumeBatch(batch)"
                >
                  恢复巡检
                </button>
                <button
                  class="link"
                  type="button"
                  :data-testid="`toggle-results-${batch.id}`"
                  @click="toggleResults(Number(batch.id))"
                >
                  {{ expandedBatches.includes(Number(batch.id)) ? '收起结果' : '窗口结果' }}
                </button>
              </td>
            </tr>
            <tr v-if="expandedBatches.includes(Number(batch.id))" class="batch-results-row">
              <td colspan="10">
                <table class="data-table batch-results-table">
                  <thead>
                    <tr>
                      <th>窗口键</th>
                      <th>主题</th>
                      <th>窗口边界</th>
                      <th>窗口状态</th>
                      <th>巡检结果</th>
                      <th>死信</th>
                      <th>去重来源批次</th>
                      <th>检查时间</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="window in batch.windows || []" :key="window.window_key">
                      <td>{{ window.window_key }}</td>
                      <td>{{ window.topic }}</td>
                      <td>{{ window['窗口边界'] }}</td>
                      <td>{{ window.status }}</td>
                      <td>{{ window.result ?? '—' }}</td>
                      <td>{{ window.dead_letter_id ? `DEAD-${String(window.dead_letter_id).padStart(4, '0')}` : '—' }}</td>
                      <td>{{ window.dedup_of ?? '—' }}</td>
                      <td>{{ window.checked_at ?? '—' }}</td>
                    </tr>
                  </tbody>
                </table>
              </td>
            </tr>
          </template>
          <tr v-if="!batches.length">
            <td colspan="10" class="empty-state">暂无巡检批次，勾选质量规则后点击「批量巡检选中规则」</td>
          </tr>
        </tbody>
      </table>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记质量规则</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条流式质量记录</span>
      <span v-if="batchMessage" class="batch-message">{{ batchMessage }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type BatchWindow = {
  window_key: string
  topic: string
  status: string
  result: string | null
  dead_letter_id: number | null
  dedup_of: string | null
  checked_at: string | null
  [key: string]: unknown
}
type Batch = {
  id: number
  status: string
  windows: BatchWindow[]
  results: Array<Record<string, unknown>>
  批次编号: string
  主题列表: string[]
  来源版本: string
  去重口径: string
  窗口总数: number
  已检查数: number
  已去重数: number
  续做边界: number
}

const ENDPOINT = '/api/stream_quality'
const columns = ["规则编号", "监控主题", "校验类型", "告警阈值", "采样比例", "检测窗口", "阻断动作", "规则版本"]
const actions = ["提交校验", "标记生效", "停用规则"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const metricNames = ["质量规则总数", "待校验规则", "已生效规则"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

// 批量巡检：多选整组处理 + 巡检批次表
const selectedIds = ref<number[]>([])
const batches = ref<Batch[]>([])
const expandedBatches = ref<number[]>([])
const batchRunning = ref(false)
const simulateInterrupt = ref(false)
const batchMessage = ref('')

const allSelected = computed(() => rows.value.length > 0 && rows.value.every((row) => selectedIds.value.includes(Number(row.id))))

function toggleSelect(id: number) {
  selectedIds.value = selectedIds.value.includes(id)
    ? selectedIds.value.filter((item) => item !== id)
    : [...selectedIds.value, id]
}

function toggleSelectAll() {
  selectedIds.value = allSelected.value ? [] : rows.value.map((row) => Number(row.id))
}

function toggleResults(batchId: number) {
  expandedBatches.value = expandedBatches.value.includes(batchId)
    ? expandedBatches.value.filter((item) => item !== batchId)
    : [...expandedBatches.value, batchId]
}

async function runBatch() {
  if (!selectedIds.value.length) return
  batchRunning.value = true
  errorMessage.value = ''
  batchMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/batch-inspections`, {
      method: 'POST',
      body: JSON.stringify({ rule_ids: selectedIds.value, simulate_interrupt: simulateInterrupt.value }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '批量巡检失败')
    batchMessage.value = payload.message || '批量巡检已提交'
    selectedIds.value = []
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批量巡检失败'
  } finally {
    batchRunning.value = false
  }
}

async function resumeBatch(batch: Batch) {
  errorMessage.value = ''
  batchMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/batch-inspections/${batch.id}/resume`, { method: 'POST' })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '恢复巡检失败')
    batchMessage.value = payload.message || '已从断点恢复巡检'
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复巡检失败'
  }
}

async function reloadBatches() {
  try {
    const response = await request(`${ENDPOINT}/batch-inspections`)
    if (!response.ok) throw new Error('巡检批次表读取失败')
    const payload = await response.json()
    batches.value = payload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '巡检批次表读取失败'
  }
}

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'stream_quality.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('流式质量动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '流式质量操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('质量规则列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '流式质量列表读取失败'
  }
  await reloadBatches()
}

onMounted(reload)
</script>

<style scoped>
.batch-bar {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 12px;
  padding: 10px 12px;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
}
.batch-toggle {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--muted);
}
.batch-hint {
  font-size: 12px;
  color: var(--muted);
}
.batch-message {
  font-size: 12px;
  color: var(--brand);
}
.batch-section {
  margin-top: 16px;
}
.batch-head {
  display: flex;
  align-items: baseline;
  gap: 12px;
  margin: 8px 0;
}
.batch-head h3 {
  margin: 0;
  font-size: 15px;
}
.batch-results-row td {
  background: #fafbfc;
  padding: 8px 12px;
}
.batch-results-table th,
.batch-results-table td {
  font-size: 12px;
}
.row-select {
  text-align: center;
}
</style>
