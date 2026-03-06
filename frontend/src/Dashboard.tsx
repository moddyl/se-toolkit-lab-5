import { useEffect, useState } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Bar, Line } from 'react-chartjs-2';

// Регистрируем компоненты Chart.js
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

// Типы для данных из API
interface ScoreBucket {
  bucket: string;
  count: number;
}

interface TimelinePoint {
  date: string;
  submissions: number;
}

interface PassRate {
  task: string;
  avg_score: number;
  attempts: number;
}

export default function Dashboard() {
  const [selectedLab, setSelectedLab] = useState('lab-04');
  const [scoreData, setScoreData] = useState<ScoreBucket[]>([]);
  const [timelineData, setTimelineData] = useState<TimelinePoint[]>([]);
  const [passRates, setPassRates] = useState<PassRate[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Базовый URL API из переменных окружения
  const API_BASE = import.meta.env.VITE_API_TARGET || 'http://localhost:42002';
  
  // Токен из localStorage (сохраняется после авторизации)
  const token = localStorage.getItem('api_key');

  // Загрузка данных при изменении выбранной лабы
  useEffect(() => {
    if (!token) {
      setError('No API key found. Please log in first.');
      setLoading(false);
      return;
    }

    const fetchData = async () => {
      setLoading(true);
      setError(null);

      try {
        const headers = {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
        };

        // Запрашиваем все три эндпоинта параллельно
        const [scoresRes, timelineRes, passRatesRes] = await Promise.all([
          fetch(`${API_BASE}/analytics/scores?lab=${selectedLab}`, { headers }),
          fetch(`${API_BASE}/analytics/timeline?lab=${selectedLab}`, { headers }),
          fetch(`${API_BASE}/analytics/pass-rates?lab=${selectedLab}`, { headers }),
        ]);

        if (!scoresRes.ok || !timelineRes.ok || !passRatesRes.ok) {
          throw new Error('Failed to fetch analytics data');
        }

        const scores = await scoresRes.json();
        const timeline = await timelineRes.json();
        const passRatesData = await passRatesRes.json();

        setScoreData(scores);
        setTimelineData(timeline);
        setPassRates(passRatesData);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [selectedLab, API_BASE, token]);

  // Подготовка данных для графика score distribution
  const scoreChartData = {
    labels: scoreData.map((item) => item.bucket),
    datasets: [
      {
        label: 'Number of submissions',
        data: scoreData.map((item) => item.count),
        backgroundColor: 'rgba(53, 162, 235, 0.5)',
      },
    ],
  };

  // Подготовка данных для графика timeline
  const timelineChartData = {
    labels: timelineData.map((item) => item.date),
    datasets: [
      {
        label: 'Submissions per day',
        data: timelineData.map((item) => item.submissions),
        borderColor: 'rgb(75, 192, 192)',
        backgroundColor: 'rgba(75, 192, 192, 0.5)',
      },
    ],
  };

  // Общие настройки для графиков
  const chartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top' as const,
      },
    },
  };

  // Состояние загрузки
  if (loading) {
    return (
      <div className="p-8 text-center">
        <div className="text-gray-600">Loading dashboard data...</div>
      </div>
    );
  }

  // Состояние ошибки
  if (error) {
    return (
      <div className="p-8 text-center">
        <div className="text-red-600">Error: {error}</div>
      </div>
    );
  }

  // Основной рендер
  return (
    <div className="p-8">
      <h1 className="text-2xl font-bold mb-6">Analytics Dashboard</h1>

      {/* Выпадающий список для выбора лабы */}
      <div className="mb-6">
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Select Lab
        </label>
        <select
          value={selectedLab}
          onChange={(e) => setSelectedLab(e.target.value)}
          className="px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
        >
          <option value="lab-04">Lab 04</option>
          <option value="lab-05">Lab 05</option>
          <option value="lab-06">Lab 06</option>
        </select>
      </div>

      {/* Сетка с графиками */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* График распределения оценок */}
        <div className="bg-white p-6 rounded-lg shadow">
          <h2 className="text-xl font-semibold mb-4">Score Distribution</h2>
          {scoreData.length > 0 ? (
            <Bar data={scoreChartData} options={chartOptions} />
          ) : (
            <p className="text-gray-500">No score data available</p>
          )}
        </div>

        {/* График временной шкалы */}
        <div className="bg-white p-6 rounded-lg shadow">
          <h2 className="text-xl font-semibold mb-4">Submissions Timeline</h2>
          {timelineData.length > 0 ? (
            <Line data={timelineChartData} options={chartOptions} />
          ) : (
            <p className="text-gray-500">No timeline data available</p>
          )}
        </div>
      </div>

      {/* Таблица с pass rates */}
      <div className="mt-8 bg-white p-6 rounded-lg shadow">
        <h2 className="text-xl font-semibold mb-4">Pass Rates by Task</h2>
        {passRates.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Task
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Average Score
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Attempts
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {passRates.map((rate, index) => (
                  <tr key={index}>
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                      {rate.task}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {rate.avg_score.toFixed(1)}%
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {rate.attempts}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="text-gray-500">No pass rates data available</p>
        )}
      </div>
    </div>
  );
}