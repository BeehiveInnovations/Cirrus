@_implementationOnly import CKRecordCoder
import CloudKit
import CloudKitCodable
import Foundation
import os.log

final class UploadRecordContext<Persistable: CloudKitCodable>: RecordModifyingContext {

  private let defaults: UserDefaults
  private let zoneID: CKRecordZone.ID
  private let logHandler: (String, OSLogType) -> Void

  private lazy var uploadBufferKey = "UPLOADBUFFER-\(zoneID.zoneName))"

  init(
    defaults: UserDefaults, zoneID: CKRecordZone.ID,
    logHandler: @escaping (String, OSLogType) -> Void
  ) {
    self.defaults = defaults
    self.zoneID = zoneID
    self.logHandler = logHandler
  }

  func buffer(_ values: [Persistable]) {
    let records: [CKRecord]
    do {
      records = try values.map { try CKRecordEncoder(zoneID: zoneID).encode($0) }
    } catch let error {
      logHandler("Failed to encode records for upload: \(String(describing: error))", .error)
      records = values.compactMap { try? CKRecordEncoder(zoneID: zoneID).encode($0) }
    }
    
    // optimization: calls setter once
    recordsToSave = records.reduce(into: [CKRecord.ID: CKRecord]()) { result, record in
      result[record.recordID] = record
    }
  }

  func removeFromBuffer(_ values: [Persistable]) {
    // Map Persistable items to CKRecords and collect their IDs
    let recordIDsToRemove = values.compactMap { try? CKRecordEncoder(zoneID: zoneID).encode($0) }.map { $0.recordID }
    
    // optimization: Get current records to save, manipulate the dictionary once
    var currentRecordsToSave = recordsToSave
    
    // Remove all entries at once based on collected record IDs
    recordIDsToRemove.forEach { currentRecordsToSave.removeValue(forKey: $0) }
    
    // Re-assign modified dictionary back to recordsToSave if recordsToSave has a setter
    // or update your storage mechanism if recordsToSave is computed based on some storage
    recordsToSave = currentRecordsToSave
  }

  // MARK: - RecordModifying

  let name = "upload"
  let savePolicy: CKModifyRecordsOperation.RecordSavePolicy = .ifServerRecordUnchanged

  var recordsToSave: [CKRecord.ID: CKRecord] {
    get {
      guard let data = defaults.data(forKey: uploadBufferKey) else { return [:] }
      do {
        return try NSKeyedUnarchiver.unarchiveTopLevelObjectWithData(data)
          as? [CKRecord.ID: CKRecord] ?? [:]
      } catch {
        logHandler("Failed to decode CKRecord.IDs from defaults key uploadBufferKey", .error)
        return [:]
      }
    }
    set {
      do {
        logHandler("Updating \(self.name) buffer with \(newValue.count) items", .info)
        let data = try NSKeyedArchiver.archivedData(
          withRootObject: newValue, requiringSecureCoding: true)
        defaults.set(data, forKey: uploadBufferKey)
      } catch {
        logHandler("Failed to encode record ids for upload: \(String(describing: error))", .error)
      }
    }
  }

  var recordIDsToDelete: [CKRecord.ID] = []

  func modelChangeForUpdatedRecords<T: CloudKitCodable>(
    recordsSaved: [CKRecord], recordIDsDeleted: [CKRecord.ID]
  ) -> SyncEngine<T>.ModelChange {
    let models: Set<T> = Set(
      recordsSaved.compactMap { record in
        do {
          let decoder = CKRecordDecoder()
          return try decoder.decode(T.self, from: record)
        } catch {
          logHandler("Error decoding item from record: \(String(describing: error))", .error)
          return nil
        }
      })

    updateRecordsToSave(with: recordsSaved)

    return .updated(models)
  }

  func failedToUpdateRecords(recordsSaved: [CKRecord], recordIDsDeleted: [CKRecord.ID]) {
    updateRecordsToSave(with: recordsSaved)
  }
  
  /// Optimized save to `recordsToSave`
  /// - Parameter recordsSaved: records to remove
  private func updateRecordsToSave(with recordsSaved: [CKRecord]) {
    // Access recordsToSave once, modify, and then update if necessary
    var currentRecordsToSave = recordsToSave
    
    recordsSaved.forEach { currentRecordsToSave.removeValue(forKey: $0.recordID) }
    
    // Assuming recordsToSave has a setter
    recordsToSave = currentRecordsToSave
  }
}
