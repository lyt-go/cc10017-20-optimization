<template>
  <section class="page" data-module="watermark_track">
    <header class="page-head">
      <div>
        <h2>水位追踪管理</h2>
        <p class="page-desc">维护流处理事件时间与处理时间的双水位追踪，为水位追踪提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记水位标记</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出水位追踪清单</button>
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

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
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
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无水位追踪数据，可先登记水位标记</td>
        </tr>
      </tbody>
    </table>

    <section class="unchecked-section" data-testid="unchecked-section">
      <header class="unchecked-head">
        <h3>未检查水位窗口</h3>
        <span class="unchecked-hint">
          来源：巡检批次表（{{ uncheckedInfo.source_table }}），已检查窗口 {{ uncheckedInfo.checked_count }} 个
        </span>
        <button class="btn ghost" data-testid="refresh-unchecked" type="button" @click="reloadUnchecked">核对窗口</button>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>标记编号</th>
            <th>数据主题</th>
            <th>检测窗口</th>
            <th>窗口边界</th>
            <th>延迟记录数</th>
            <th>水位状态</th>
            <th>规则版本</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="window in uncheckedInfo.unchecked" :key="`${window['标记编号']}-${window['窗口边界']}`" :data-testid="`unchecked-${window['标记编号']}`">
            <td>{{ window['标记编号'] }}</td>
            <td>{{ window['数据主题'] }}</td>
            <td>{{ window['检测窗口'] }}</td>
            <td>{{ window['窗口边界'] }}</td>
            <td>{{ window['延迟记录数'] }}</td>
            <td>{{ window['水位状态'] }}</td>
            <td>{{ window['规则版本'] }}</td>
          </tr>
          <tr v-if="!uncheckedInfo.unchecked.length">
            <td colspan="7" class="empty-state">所有水位窗口均已在巡检批次表中检查</td>
          </tr>
        </tbody>
      </table>
      <p v-if="uncheckedInfo.source_batches.length" class="unchecked-batches">
        批次来源：
        <span v-for="batch in uncheckedInfo.source_batches" :key="batch['批次编号']" class="unchecked-batch-tag">
          {{ batch['批次编号'] }}（{{ batch['来源版本'] }} · {{ batch['去重口径'] }} · {{ batch.status }}）
        </span>
      </p>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记水位标记</h3>
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
      <span>共 {{ total }} 条水位追踪记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type UncheckedWindow = Record<string, string | number | null>
type UncheckedInfo = {
  source_table: string
  unchecked: UncheckedWindow[]
  unchecked_count: number
  checked_count: number
  source_batches: Array<Record<string, string>>
}

const ENDPOINT = '/api/watermark_track'
const columns = ["标记编号", "数据主题", "分区编号", "事件时间水位", "处理时间水位", "延迟记录数", "消费偏移", "检查时间"]
const actions = ["标记正常", "标记滞后", "停用追踪"]
const statuses = ["草稿", "正常", "滞后", "已停用"]
const metricNames = ["水位标记总数", "正常水位", "滞后水位"]

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

// 未检查水位窗口：跨模块接收流式质量的巡检批次表
const uncheckedInfo = ref<UncheckedInfo>({
  source_table: 'quality_inspection_batches',
  unchecked: [],
  unchecked_count: 0,
  checked_count: 0,
  source_batches: [],
})

async function reloadUnchecked() {
  try {
    const response = await request(`${ENDPOINT}/unchecked-windows`)
    if (!response.ok) throw new Error('未检查水位窗口读取失败')
    uncheckedInfo.value = await response.json()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '未检查水位窗口读取失败'
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
    link.download = 'watermark_track.csv'
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
      throw new Error('水位追踪动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '水位追踪操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('水位标记列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '水位追踪列表读取失败'
  }
  await reloadUnchecked()
}

onMounted(reload)
</script>

<style scoped>
.unchecked-section {
  margin-top: 16px;
}
.unchecked-head {
  display: flex;
  align-items: baseline;
  gap: 12px;
  margin: 8px 0;
}
.unchecked-head h3 {
  margin: 0;
  font-size: 15px;
}
.unchecked-hint {
  font-size: 12px;
  color: var(--muted);
}
.unchecked-batches {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  font-size: 12px;
  color: var(--muted);
  margin: 8px 0 0;
}
.unchecked-batch-tag {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 2px 8px;
}
</style>
