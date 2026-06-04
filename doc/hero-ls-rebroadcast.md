# hero-ls-rebroadcast

## Краткое описание

`hero-ls-rebroadcast` добавляет к `master` режим hero-rebroadcaster'а для full node / validator-engine.
Основная задача ветки - принимать блоки из custom overlay или из принудительной догрузки, подписывать их
подходящим shard overlay certificate и рассылать в публичный shard overlay так, чтобы обычные ноды принимали
эти сообщения как валидные overlay broadcasts и дальше распространяли их штатным механизмом.

Ключевой путь данных:

- custom overlay получает `tonNode_blockBroadcast`, `tonNode_blockBroadcastCompressed` или
  `tonNode_newBlockCandidate` и помечает источник как `BroadcastSource::custom_overlay`;
- `FullNodeImpl` проверяет разрешенные workchain'ы, дедуплицирует уже отправленные блоки/кандидаты и планирует
  public rebroadcast;
- `FullNodeShardImpl` выбирает ADNL source, сертификат, force-good peers и fanout target;
- overlay FEC отправляет broadcast с custom fanout: сначала force-good peers, затем обычные известные соседи
  до достижения target fanout;
- validator-engine может автоматически выдавать и импортировать shard overlay certificates для rebroadcaster'а.

Отдельно от rebroadcast-логики в ветке есть унаследованные изменения по TL/network metrics и tontester dashboard.
Они видны в diff к `master`, но не являются обязательной частью минимального hero-rebroadcaster path.

## Опции ноды

Опции добавлены в `validator-engine`.

| Опция | Назначение |
| --- | --- |
| `--rebroadcast-from-custom` | Включает реброадкаст block broadcasts, полученных из custom overlays, в публичные overlays. Также включает использование ADNL id как broadcast source. |
| `--rebroadcast-candidates-from-custom` | Дополнительно реброадкастит `newBlockCandidate` из custom overlays. Требует `--rebroadcast-from-custom`. |
| `--broadcast-candidate-block-dedup` | Если candidate для блока уже был реброадкастнут, подавляет последующий public block rebroadcast этого же блока. Требует `--rebroadcast-candidates-from-custom`. |
| `--rebroadcast-peers N` | Fanout target для public rebroadcast. По умолчанию `100`, значение должно быть положительным. |
| `--force-good-peers URL` | HTTP URL со списком ADNL peers, которые надо принудительно включать в rebroadcast fanout. Требует включенный public rebroadcast или downloaded rebroadcast. Поддерживается только `http://`. |
| `--force-download-peers FILE` | Файл с hex ADNL ids, по одному на строку, для принудительной загрузки недостающих блоков. Дубликаты по ADNL id удаляются при чтении. |
| `--download-attempts-num N` | Сколько force-download peers запрашивать параллельно для одного недостающего блока. По умолчанию `1`; если опция задана явно, требует `--force-download-peers`. |
| `--rebroadcast-downloaded-block` | Реброадкастит блоки, которые были получены через network download. Для masterchain нужен полный proof и signatures; proof-link не реброадкастится как masterchain block broadcast. |
| `--rebroadcast-workchains LIST` | Обязательная опция для public rebroadcast. Поддерживает только `0`, `-1` или список через запятую, например `0,-1`. `0` - basechain, `-1` - masterchain. |
| `--auto-sign ADNL` | На validator/issuer ноде автоматически выпускает shard overlay certificates для указанного ADNL rebroadcaster'а. |
| `--accept-certs-from ADNL` | На rebroadcaster'е принимает shard overlay certificates от указанного sender ADNL. |
| `--accept-certs-from *` | Принимает shard overlay certificates от любого валидаторского issuer'а. |

Типовая rebroadcaster-конфигурация для basechain и masterchain:

```text
--rebroadcast-from-custom
--rebroadcast-candidates-from-custom
--broadcast-candidate-block-dedup
--rebroadcast-workchains 0,-1
--rebroadcast-peers 300
--force-good-peers http://host:port/
--accept-certs-from <issuer-adnl-hex>
```

Если нужно добирать недостающие блоки вручную и тоже отправлять их наружу:

```text
--force-download-peers /path/to/peers.txt
--download-attempts-num 3
--rebroadcast-downloaded-block
```

На стороне validator/issuer ноды:

```text
--auto-sign <rebroadcaster-adnl-hex>
```

`force-good-peers` загружается обычным HTTP GET с `Accept: application/json` и
`User-Agent: ton-validator-force-good-peers`. Ответ должен быть JSON object:

```json
{
  "<adnl-short-id-hex>": {
    "host": "203.0.113.10",
    "port": 30303,
    "pub_key": "<base64-ed25519-public-key>"
  }
}
```

Ключ JSON object'а - 32-byte ADNL short id в hex. `pub_key` должен соответствовать этому ADNL id. Поддерживаются только
IPv4 UDP адреса. Список дедуплицируется по ADNL id, не по IP. Успешный refresh повторяется примерно через 50-70
секунд, ошибка - через 10-20 секунд; timeout HTTP запроса - 60 секунд.

Выбор peers для rebroadcast:

- если `force_good_peers.size() <= fanout_target`, берутся все force-good peers;
- если `force_good_peers.size() > fanout_target`, выбирается `fanout_target` случайных peers без повторений;
- если force-good peers меньше target fanout, оставшиеся места добираются из обычных overlay neighbours;
- на этапе отправки есть дедуп по ADNL id и пропуск локального ADNL id, IP-level dedup нет.

## Расширенное описание достаточное чтобы в следующий раз быстрее мержить и накатывать с мастером

### Основные файлы rebroadcast-логики

- `validator-engine/validator-engine.cpp`, `validator-engine/validator-engine.hpp`
  - CLI options;
  - валидация зависимостей опций;
  - auto-sign/import shard overlay certificates;
  - periodic scan раз в 60 секунд для выпуска сертификатов;
  - certificate expiry сейчас выставляется примерно на 1 час, `max_size` берется из
    `overlay::Overlays::max_fec_broadcast_size()`.
- `validator/full-node.h`, `validator/full-node.hpp`, `validator/full-node.cpp`
  - `FullNodeOptions::RebroadcastFromCustomOptions` и `ForceDownloadOptions`;
  - `BroadcastSource`-aware обработка входящих block/candidate broadcasts;
  - дедуп `custom_to_public_sent_blocks_` и `custom_to_public_sent_candidates_`;
  - логика `custom-to-public` и `downloaded-to-public`.
- `validator/full-node-custom-overlays.cpp`
  - входящие custom overlay broadcasts передаются в full node как `BroadcastSource::custom_overlay`.
- `validator/full-node-fast-sync-overlays.cpp`
  - входящие fast sync broadcasts помечаются как `BroadcastSource::fast_sync_overlay`, чтобы их не путать с custom path.
- `validator/full-node-shard.cpp`, `validator/full-node-shard.hpp`, `validator/full-node-shard.h`
  - HTTP fetcher для `force-good-peers`;
  - выбор force-good peers и force-download peers;
  - `send_broadcast_with_fanout` и `send_block_candidate_with_fanout`;
  - выбор outbound source и проверка, что есть сертификат, который покрывает payload size;
  - import/accept shard overlay certificate для конкретного shard.
- `overlay/overlays.h`, `overlay/overlay-manager.*`, `overlay/overlay.*`
  - новый API `send_broadcast_fec_ex_with_fanout` / `send_broadcast_fec_with_fanout`;
  - обычные `send_broadcast_fec*` продолжают делегировать в новый API с `fanout_override = 0`, поэтому штатный path
    без rebroadcast опций остается прежним.
- `overlay/broadcast-fec.*`
  - FEC broadcast получает `fanout_override` и `force_peers`;
  - outbound rebroadcast с `fanout_override != 0` считается trusted outbound только для локально подписанной отправки,
    а inbound certificate validation остается обычной;
  - logs по signing, selected peers и FEC parts.
- `validator/net/download-block-new.*`
  - `DownloadedBlock` содержит proof metadata, `sig_set`, `cc_seqno`, `validator_set_hash` и `from_network`;
  - `DownloadBlockNewParallel` запускает несколько download attempts и возвращает первый успешный результат.
- `validator/types.h`
  - общие структуры `DownloadedBlock`, `BlockBroadcast`, `BroadcastSource`.

### Поведение по workchain'ам

`--rebroadcast-workchains 0` включает только basechain. Чтобы также реброадкастить masterchain blocks, нужен
`--rebroadcast-workchains 0,-1`.

Candidate broadcasts для basechain отправляются через masterchain public overlay, поэтому при включенных candidates
masterchain shard actor тоже использует force-good-peers даже если разрешен только workchain `0`. Для downloaded
rebroadcast masterchain blocks отправляются как полноценный `BlockBroadcast` только если есть полный proof и signatures;
basechain downloaded blocks отправляются наружу как candidate broadcasts.

### Сертификаты

Public rebroadcast должен иметь валидный shard overlay certificate для ADNL source, иначе обычные ноды отвергнут
broadcast как forbidden. Ветка решает это двумя сторонами:

- issuer/validator с `--auto-sign <rebroadcaster-adnl>` раз в минуту сканирует shards и отправляет rebroadcaster'у
  `engine_validator_importShardOverlayCertificate`;
- rebroadcaster с `--accept-certs-from <issuer-adnl>` или `--accept-certs-from *` импортирует сертификаты в full node
  shard actor.

Важные logs для диагностики:

- `public rebroadcast cert issue scan`
- `public rebroadcast cert issued`
- `public rebroadcast cert received`
- `public rebroadcast cert import scheduled`
- `public rebroadcast cert accepted`
- `public rebroadcast cert imported`
- `public rebroadcast dispatch`
- `public rebroadcast scheduled`
- `public rebroadcast fec signed`
- `public rebroadcast dropped reason=invalid-certificate`

Низкообъемные ключевые diagnostics подняты на `LOG(WARNING)` и видны на v2. Более частые per-part/per-attempt details
остаются на `LOG(INFO)`, например `public rebroadcast fec part sent` и forced download attempt success/failure.

### Force-good peers и fanout

`force-good-peers` нужен не для замены overlay neighbour discovery, а для bias в сторону известных хороших публичных
узлов. Список сначала добавляется в ADNL как peers с UDP address list, затем при каждой отправке выбирается подмножество
до `fanout_target`. Если force-good peers меньше target, остаток добирается из `overlay->get_neighbours(fanout)`.

При merge важно сохранить именно это свойство: force-good peers идут первыми, но не отключают обычный overlay fanout.
Иначе при малом или временно пустом force-good списке rebroadcaster перестанет рассылать наружу.

### Force-download и downloaded-to-public

`--force-download-peers` используется в `FullNodeShardImpl::download_block` и `get_next_block`, когда full node работает
без external client. Вместо одного выбранного peer запускается `DownloadBlockNewParallel`, который делает до
`--download-attempts-num` параллельных запросов к случайным peers из файла. Первый успешный результат завершает query,
остальные attempts отменяются.

Если включен `--rebroadcast-downloaded-block`, `finish_download_block` передает `DownloadedBlock` в `FullNodeImpl`.
Дальше применяется тот же public rebroadcast gate по `--rebroadcast-workchains`.

### Что проверять при следующем merge с master

1. Сначала сохранить master-side изменения, не относящиеся к rebroadcast. В прошлых merge больше всего конфликтовали
   consensus/validator-session/catchain, QUIC/ADNL и metrics; их не надо тащить назад из старой ветки, если master уже
   поменял архитектуру.
2. Сверить, что `overlay` API все еще имеет обе формы:
   обычный `send_broadcast_fec*` с `fanout_override = 0` и rebroadcast-only API с явным fanout/force peers.
3. Проверить, что inbound certificate hardening из master не ослаблен. Ветка должна доверять только локальному outbound
   rebroadcast path, а не принимать чужие плохие certificates.
4. Проверить `ValidatorManagerInterface` и download API. В текущем merge потребовалось адаптировать renamed/async методы
   вокруг `got_next_masterchain_block`, `send_get_block_request` и `new_block_broadcast`.
5. Проверить `FullNodeShardImpl::create_overlay`: публичный full-node shard ADNL не регистрируется в legacy RLDP, используется
   RLDP2.
6. После разрешения конфликтов собрать хотя бы:

```text
cmake -S . -B build -GNinja -DCMAKE_BUILD_TYPE=Debug
cmake --build build --target validator-engine -j4
```

7. Для runtime smoke test смотреть logs с v2:
   certificates issued/imported/accepted, `force-good-peers startup/refresh/configured`, `public rebroadcast scheduled`,
   `public rebroadcast dispatch`, `public rebroadcast fec signed`, `forced block download start` и
   `block download finished`.
