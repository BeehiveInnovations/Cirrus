import CloudKit
import Foundation
import os.log

extension SyncEngine {

  // MARK: - Internal

  func initializeZone(with queue: OperationQueue) -> Bool {
    self.createCustomZoneIfNeeded()
    
    guard self.createdCustomZone else { return false }
    return true
  }

  // MARK: - Private

  private var createdCustomZone: Bool {
    get {
      return defaults.bool(forKey: createdCustomZoneKey) || defaults.bool(forKey: legacyCreatedCustomZoneKey)
    }
    set {
      defaults.set(newValue, forKey: createdCustomZoneKey)
      defaults.removeObject(forKey: legacyCreatedCustomZoneKey)
    }
  }

  private func createCustomZoneIfNeeded() {
    guard !createdCustomZone else {
      logHandler("Already have custom zone, skipping creation but checking if zone really exists", .debug)

      checkCustomZone()

      return
    }

    logHandler("Creating CloudKit zone \(zoneIdentifier.zoneName)", .info)

    let zone = CKRecordZone(zoneID: zoneIdentifier)
    let operation = CKModifyRecordZonesOperation(
      recordZonesToSave: [zone],
      recordZoneIDsToDelete: nil
    )

    operation.modifyRecordZonesCompletionBlock = { [weak self] _, _, error in
      guard let self else { return }

      if let error {
        self.logHandler(
          "Failed to create custom CloudKit zone: \(String(describing: error))", .error)

        error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
          self.createCustomZoneIfNeeded()
        }
      } 
      else {
        self.logHandler("Zone created successfully", .info)
        self.createdCustomZone = true
      }
    }

    operation.qualityOfService = .userInitiated
    operation.database = privateDatabase

    cloudOperationQueue.addOperation(operation)
    
    // Wait for operation to complete
    cloudOperationQueue.waitUntilAllOperationsAreFinished()
  }

  private func checkCustomZone() {
    let operation = CKFetchRecordZonesOperation(recordZoneIDs: [zoneIdentifier])

    operation.fetchRecordZonesCompletionBlock = { [weak self] ids, error in
      guard let self else { return }

      if let error {
        self.logHandler("Failed to check for custom zone existence: \(String(describing: error))", .error)

        if !error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue, with: { self.checkCustomZone() }) {
          
          self.logHandler("Irrecoverable error when fetching custom zone, assuming it doesn't exist: \(String(describing: error))", .error)

          self.workQueue.async { [weak self] in
            guard let self else { return }
            
            self.createdCustomZone = false
            self.createCustomZoneIfNeeded()
          }
        }
      } 
      else if ids?.isEmpty ?? true {
        self.logHandler("Custom zone reported as existing, but it doesn't exist. Creating.", .error)
        
        self.workQueue.async { [weak self] in
          guard let self else { return }
          
          self.createdCustomZone = false
          self.createCustomZoneIfNeeded()
        }
      }
      else {
        self.workQueue.async { [weak self] in
          guard let self else { return }
          
          // If checking failed earlier, we set this flag to false, must reset to true if zone in fact exists, otherwise we'll keep
          // trying to create the zone, which will always throw an error
          if self.createdCustomZone == false {
            self.createdCustomZone = true
            
            self.logHandler("Custom zone exists, updating flag", .debug)
          }
        }
      }
    }

    operation.qualityOfService = .userInitiated
    operation.database = privateDatabase

    cloudOperationQueue.addOperation(operation)
    
    // Wait for operation to complete
    cloudOperationQueue.waitUntilAllOperationsAreFinished()
  }
}
