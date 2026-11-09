package sa.factory.maherfieldservices.data.repository

import sa.factory.maherfieldservices.domain.repository.MaherFieldRepository
import sa.factory.maherfieldservices.hardware.HardwareSensorDriver
import sa.factory.maherfieldservices.model.MaherFieldItem
import sa.factory.maherfieldservices.model.MaherFieldOrder
import sa.factory.maherfieldservices.model.MaherFieldTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class MaherFieldRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : MaherFieldRepository {

    private val _catalog = MutableStateFlow<List<MaherFieldItem>>(listOf(
        MaherFieldItem("1", "امتثال زاتك الشهري", "إعداد وتقديم إقرارات الضريبة الشهرية مع تقارير Qoyod المتكاملة", 500.0, "معتمد", "ضريبة القيمة المضافة"),
        MaherFieldItem("2", "معالجة الفواتير الفورية", "إدخال فواتير المشتريات عبر NFC مدى مع تتبع GPS للتوصيل", 350.0, "فوري", "إدخال الفواتير"),
        MaherFieldItem("3", "تحليل الربح/الخسارة المزدوج", "تقارير مالية منفصلة لنشاطي المنصة والخياطة مع مؤشرات الأداء", 750.0, "جديد", "تقارير مالية")
    ))
    override val catalogFlow: Flow<List<MaherFieldItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<MaherFieldItem>>(emptyList())
    override val activeCart: StateFlow<List<MaherFieldItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<MaherFieldTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            MaherFieldTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: MaherFieldItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: MaherFieldItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): MaherFieldOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = MaherFieldOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
